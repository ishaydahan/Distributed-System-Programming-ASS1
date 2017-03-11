import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;


public class workerManagerLocal extends manager  implements Runnable {


	public workerManagerLocal(String s) {
		super(s);

	}


	public void run(){
		try{
			while(true){
				//Receive message from worker
				System.out.print(".");        		

				ReceiveMessageRequest reciveFromWorkers = new ReceiveMessageRequest();
				reciveFromWorkers.withQueueUrl(worker_manager);
				List<Message> messagesFromWorkers = sqs.receiveMessage(reciveFromWorkers.withMessageAttributeNames("All")).getMessages();
				for (Message message : messagesFromWorkers) {          		
					System.out.println("MANAGER - got message from worker");    
					Map<String, MessageAttributeValue> attributes = message.getMessageAttributes();
					MessageAttributeValue local = attributes.get("local");
					job curr = jobs.get(local.getStringValue());
					if(message.getBody().contains("finished")){
						curr.tasks--;
						if (curr.tasks>=0) totalTasks--;
						if (curr.tasks==0){
							System.out.println("MANAGER - got all info for local task");    
							// Send a message if counter == 0
							SendMessageRequest send = new SendMessageRequest();
							send.withMessageBody(curr.local);
							send.withQueueUrl(manager_local);
							send.setMessageGroupId("messageGroup1");
							sqs.sendMessage(send);    	                
						}
						// Delete the message
						System.out.println("MANAGER - deleting message of worker");        		            		
						String messageReceiptHandle = message.getReceiptHandle();
						sqs.deleteMessage(new DeleteMessageRequest(worker_manager, messageReceiptHandle));
						continue;
					}
					MessageAttributeValue ans = attributes.get("ans");
					if (curr.answers.contains(ans.getStringValue())){
						// Delete the message
						System.out.println("MANAGER - DOUBLE MESSAGE - deleting message of worker");        		            		
						String messageReceiptHandle = message.getReceiptHandle();
						sqs.deleteMessage(new DeleteMessageRequest(worker_manager, messageReceiptHandle));
						continue;
					}
					curr.answers.add(ans.getStringValue());
					JsonReader reader = new JsonReader();
					JSONObject json = reader.readJsonFromUrl("https://s3.amazonaws.com/ishay/"+local.getStringValue());
					JSONArray arr = (JSONArray) json.get("list"); 
					arr.put(ans.getStringValue());
					try {
						FileWriter file = new FileWriter(local.getStringValue()+".json");
						file.write(json.toString());
						file.flush();
						file.close();

					} catch (IOException e) {
						e.printStackTrace();
					}

					File output = new File(local.getStringValue()+".json");

					//upload file
					System.out.println("MANAGER - upload new line from worker");    
					PutObjectRequest por = new PutObjectRequest(bucketName, local.getStringValue() ,output);
					por.withCannedAcl(CannedAccessControlList.PublicRead);
					S3.putObject(por);

					// Delete the message
					System.out.println("MANAGER - deleting message of worker");        		            		
					String messageReceiptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(worker_manager, messageReceiptHandle));
				}

				for(int i=totalTasks; i<numOfWorkers; numOfWorkers--){
					System.out.println("MANAGER - relesing workers");    
					// Send a message
					System.out.println("MANAGER - sending relese message");        		
					SendMessageRequest relese = new SendMessageRequest();
					relese.withMessageBody("goToSleep"+counter);
					counter++;
					relese.withQueueUrl(manager_worker);
					relese.setMessageGroupId("messageGroup1");
					sqs.sendMessage(relese);
				}            	
			}//while
		}//try
		catch(Exception e){
			//throw exception
		}
	}//run
}
