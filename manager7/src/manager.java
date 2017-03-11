import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;


public abstract class manager implements Runnable{
	String threadName="";
	public manager(String s){
		this.threadName=s;
	}
	
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "ishay";
	public static AmazonSQSClient sqs;
	public static AmazonEC2 ec2;

	public static String manager_worker = "https://sqs.us-east-2.amazonaws.com/605668889946/manager-worker.fifo";
	public static String worker_manager = "https://sqs.us-east-2.amazonaws.com/605668889946/worker-manager.fifo";
	public static String manager_local = "https://sqs.us-east-2.amazonaws.com/605668889946/manager-local.fifo";
	public static String local_manager = "https://sqs.us-east-2.amazonaws.com/605668889946/local-manager.fifo";
	
	public static HashMap<String, job> jobs = new HashMap<String, job>();
	public static int counter =1;//id of messages
	public static int numOfWorkers =0;
	public static String myEC2Id;
	public static int totalTasks=0;
	public static boolean imAlive = true;
	
	public static void main(String[] args) throws Exception{
	    //connect to server
        System.out.println("MANAGER - connecting to servers");        	    		
		Credentials = new PropertiesCredentials(manager.class.getResourceAsStream("ishay.properties"));
		S3 = new AmazonS3Client(Credentials);
        sqs = new AmazonSQSClient(Credentials);
        sqs.setEndpoint("https://sqs.us-east-2.amazonaws.com");       
        ec2 = new AmazonEC2Client(Credentials);
        
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Runnable localManagerWorker = new localManagerWorker("localManagerWorkerThread");
        Runnable workerManagerLocal = new workerManagerLocal("workerManagerLocalThread");
        executor.execute(localManagerWorker);
        executor.execute(workerManagerLocal);
        
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");
        
	}
	public abstract void run();
}


        /*
        while(true){
            // Receive messages from local
            System.out.print(".");        		
            ReceiveMessageRequest recive = new ReceiveMessageRequest();
            recive.withQueueUrl(local_manager);
        	List<Message> messages = sqs.receiveMessage(recive.withMessageAttributeNames("All")).getMessages();
        	for (Message message : messages) {
            	if (message.getBody().contains("goToSleep")){
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
            	}
            	if (message.getBody().contains("request")){
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
*/