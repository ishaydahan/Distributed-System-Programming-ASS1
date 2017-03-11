import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;


public class localManagerWorker extends manager  implements Runnable {
	String threadName="";
	public localManagerWorker(String s) {
		super(s);
		
	}
	public void run(){
		try{
			while(true){
				// Receive messages from local
				System.out.print(".");        		
				ReceiveMessageRequest recive = new ReceiveMessageRequest();
				recive.withQueueUrl(local_manager);
				List<Message> messages = sqs.receiveMessage(recive.withMessageAttributeNames("All")).getMessages();
				for (Message message : messages) {
					if (message.getBody().contains("goToSleep")){
						if (totalTasks==0){
							// Delete the message
							System.out.println("MANAGER - deleting relese message of local");        		            		
							String messageReceiptHandle = message.getReceiptHandle();
							sqs.deleteMessage(new DeleteMessageRequest(local_manager, messageReceiptHandle));

							System.out.println("MANAGER - SAYING GOODBAY!");  
							myEC2Id = retrieveInstanceId();
							TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest();
							terminateRequest.withInstanceIds(myEC2Id); 
							ec2.terminateInstances(terminateRequest);
							return;
						}else{
							imAlive=false;
						}
					}
					if (message.getBody().contains("request")&& imAlive){
						System.out.println("MANAGER - got message from local.. analazing");        		
						Map<String, MessageAttributeValue> attributes = message.getMessageAttributes();
						MessageAttributeValue AccountId = attributes.get("AccountId");
						MessageAttributeValue n = attributes.get("n");
						MessageAttributeValue d = attributes.get("d");

						JsonReader reader = new JsonReader();
						JSONObject json = reader.readJsonFromUrl("https://s3.amazonaws.com/ishay/"+AccountId.getStringValue());

						jobs.put(AccountId.getStringValue(), new job(
								AccountId.getStringValue(), 
								Integer.parseInt(n.getStringValue()), 
								Integer.parseInt(d.getStringValue()), 
								(String)json.get("start-date"), 
								(String)json.get("end-date"),
								(Double)json.getDouble("speed-threshold"), 
								(Double)json.getDouble("diameter-threshold"), 
								(Double)json.getDouble("miss-threshold")
								));

						System.out.println("MANAGER - deleting file of local");        		            		
						S3.deleteObject(new DeleteObjectRequest(bucketName, AccountId.getStringValue()));

						JSONObject result = new JSONObject();
						JSONArray list = new JSONArray();
						result.put("list", list);
						try {
							FileWriter file = new FileWriter(AccountId.getStringValue()+".json");
							file.write(result.toString());
							file.flush();
							file.close();

						} catch (IOException e) {
							e.printStackTrace();
						}

						File output = new File(AccountId.getStringValue()+".json");

						//upload file
						System.out.println("MANAGER - uploading output file of local");        		            		
						PutObjectRequest por = new PutObjectRequest(bucketName, AccountId.getStringValue() ,output);
						por.withCannedAcl(CannedAccessControlList.PublicRead);
						S3.putObject(por);

						// Delete the message
						System.out.println("MANAGER - deleting message of local");        		            		
						String messageReceiptHandle = message.getReceiptHandle();
						sqs.deleteMessage(new DeleteMessageRequest(local_manager, messageReceiptHandle));

						System.out.println("MANAGER - start spreading tasks");        		            		
						start_working(AccountId.getStringValue());
					}
				}
			}
		}catch(Exception e){
			//throw exception
		}
	}
	public static void start_working(String AccountId) throws Exception{
		job myjob = jobs.get(AccountId);
		LocalDate start = LocalDate.parse(myjob.start_date);
		LocalDate end = LocalDate.parse(myjob.end_date);
		int days = Days.daysBetween(start, end).getDays() +1 ;
		int periods = days/myjob.d;
		if (days%periods !=0) periods++;
		myjob.tasks = periods;
		int computers = periods/myjob.n;
		myjob.computers = computers;
		totalTasks = totalTasks + periods;
		String startDayForWorker = myjob.start_date;

		for (int i=0; i< periods ; i++){
			// Send a message to worker
			System.out.println("MANAGER - sending new task message");        		            		
			SendMessageRequest send = new SendMessageRequest();
			send.withMessageBody("task"+counter);
			counter++;
			send.withQueueUrl(manager_worker);
			send.setMessageGroupId("messageGroup1");
			Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
			messageAttributes.put("d", new MessageAttributeValue().withDataType("String").withStringValue(myjob.d+""));
			messageAttributes.put("start_date", new MessageAttributeValue().withDataType("String").withStringValue(startDayForWorker));
			messageAttributes.put("speed", new MessageAttributeValue().withDataType("String").withStringValue(myjob.speed+""));
			messageAttributes.put("diameter", new MessageAttributeValue().withDataType("String").withStringValue(myjob.diameter+""));
			messageAttributes.put("miss", new MessageAttributeValue().withDataType("String").withStringValue(myjob.miss+""));
			messageAttributes.put("local", new MessageAttributeValue().withDataType("String").withStringValue(myjob.local));
			messageAttributes.put("end_date", new MessageAttributeValue().withDataType("String").withStringValue(myjob.end_date));

			send.withMessageAttributes(messageAttributes);
			sqs.sendMessage(send);

			SendMessageResult sendMessageResult = sqs.sendMessage(send);
			String sequenceNumber = sendMessageResult.getSequenceNumber();
			String messageId = sendMessageResult.getMessageId();
			System.out.println("SendMessage succeed with messageId " + messageId + ", sequence number " + sequenceNumber + "");

			start = start.plusDays(myjob.d);
			startDayForWorker = start.toString();
		}
		for (int i=numOfWorkers; i< computers ; i++){
			System.out.println("MANAGER - create new worker");    
			numOfWorkers++;

			RunInstancesRequest request = new RunInstancesRequest();
			request.setInstanceType(InstanceType.T2Micro.toString());
			request.setMinCount(1);
			request.setMaxCount(1);
			request.setImageId("ami-b66ed3de");
			request.setKeyName("ishay");
			request.setUserData(getUserDataScript());
			ec2.runInstances(request);    
		}
	}

	public static String getUserDataScript(){
		ArrayList<String> lines = new ArrayList<String>();
		lines.add("#! /bin/bash");
		lines.add("wget https://s3.amazonaws.com/ishay/worker.zip -O worker.zip");
		lines.add("unzip -P 12661266 worker.zip");
		lines.add("java -jar worker.jar"); 
		String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
		return str;
	}

	public static String join(Collection<String> s, String delimiter) {
		StringBuilder builder = new StringBuilder();
		Iterator<String> iter = s.iterator();
		while (iter.hasNext()) {
			builder.append(iter.next());
			if (!iter.hasNext()) {
				break;
			}
			builder.append(delimiter);
		}
		return builder.toString();
	}

	public static String retrieveInstanceId() throws Exception {
		String EC2Id = "";
		String inputLine;
		URL EC2MetaData = new URL("http://169.254.169.254/latest/meta-data/instance-id");
		URLConnection EC2MD = EC2MetaData.openConnection();
		BufferedReader in = new BufferedReader(new InputStreamReader(EC2MD.getInputStream()));
		while ((inputLine = in.readLine()) != null){	
			EC2Id = inputLine;
		}
		in.close();
		return EC2Id;
	}




}
