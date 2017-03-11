import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.joda.time.LocalDate;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class worker extends Thread {
	
	public static String manager_worker = "https://sqs.us-east-2.amazonaws.com/605668889946/manager-worker.fifo";
	public static String worker_manager = "https://sqs.us-east-2.amazonaws.com/605668889946/worker-manager.fifo";

	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "ishay";
	public static AmazonSQSClient sqs;
	public static AmazonEC2 ec2;

	public static int d;
	public static String start_date;
	public static String end_date;
	public static String local;
	public static double speed;
	public static double diameter;
	public static double miss;
	
	public static String ans = null;
	public static String myMessage;
	public static String myEC2Id;

	public static void main (String args[]) throws Exception{
	
	    //connect to server
        System.out.println("WORKER - connecting to servers");   
        
		Credentials = new PropertiesCredentials(worker.class.getResourceAsStream("ishay.properties"));
		S3 = new AmazonS3Client(Credentials);
        sqs = new AmazonSQSClient(Credentials);
        sqs.setEndpoint("https://sqs.us-east-2.amazonaws.com");       
        ec2 = new AmazonEC2Client(Credentials);

        while (true){
            // Receive messages
            System.out.println("WORKER - waiting for messages");        		
            ReceiveMessageRequest recive = new ReceiveMessageRequest();
            recive.withQueueUrl(manager_worker);
			recive.withMaxNumberOfMessages(1);
        	List<Message> messages = sqs.receiveMessage(recive.withMessageAttributeNames("All")).getMessages();
        	for (Message message : messages) {
            	if (message.getBody().contains("goToSleep")){
                	
            		
            		
            		// Delete the message
                    System.out.println("WORKER - deleting relese message of manager");        		            		
                	String messageReceiptHandle = message.getReceiptHandle();
                	sqs.deleteMessage(new DeleteMessageRequest(manager_worker, messageReceiptHandle));
                    System.out.println("WORKER - SAYING GOODBAY!");  
                    myEC2Id = retrieveInstanceId();
                    TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest();
                    terminateRequest.withInstanceIds(myEC2Id); 
                    ec2.terminateInstances(terminateRequest);
            		return;
            	}
            	if (message.getBody().contains("task")){
            		myMessage = message.getBody();
                    System.out.println("WORKER - got message");        		
            	    Map<String, MessageAttributeValue> attributes = message.getMessageAttributes();
            	    MessageAttributeValue M_d = attributes.get("d");
            	    MessageAttributeValue M_start_date = attributes.get("start_date");
            	    MessageAttributeValue M_end_date = attributes.get("end_date");
            	    MessageAttributeValue M_speed = attributes.get("speed");  
            	    MessageAttributeValue M_diameter = attributes.get("diameter");
            	    MessageAttributeValue M_miss = attributes.get("miss"); 
            	    MessageAttributeValue M_local = attributes.get("local");  
            	    d = Integer.parseInt(M_d.getStringValue());
            	    start_date = M_start_date.getStringValue();
            	    end_date = M_end_date.getStringValue();
            	    speed = Double.parseDouble(M_speed.getStringValue());
            	    diameter = Double.parseDouble(M_diameter.getStringValue());
            	    miss = Double.parseDouble(M_miss.getStringValue());
            	    local = M_local.getStringValue();        	    
            	    try {
						work();
					} catch (JSONException | IOException e) {
						e.printStackTrace();
					}
                	finished();
                	// Delete the message
                    System.out.println("WORKER - deleting message of manager");        		            		
                	String messageReceiptHandle = message.getReceiptHandle();
                	sqs.deleteMessage(new DeleteMessageRequest(manager_worker, messageReceiptHandle));
            	}				
        	}	
    	}	
    }
	
    public static void work() throws JSONException, IOException{
		// parse date from yyyy-mm-dd pattern
		LocalDate mydate = LocalDate.parse(start_date);
		String the_end_date = mydate.plusDays(d-1).toString();
		
		//JSON from URL to Object
		JsonReader reader = new JsonReader();
	    JSONObject json = reader.readJsonFromUrl("https://api.nasa.gov/neo/rest/v1/feed?start_date="+start_date+"&end_date="+the_end_date+"&api_key=YP10OlXw8wD6ZlOPBgIHSZOpEKKLdkypstRhPk5Y");
		
	    // loop array
	    JSONObject near_earth_objects = (JSONObject) json.get("near_earth_objects");
	    while (d>0){
	        System.out.println("WORKER - calculating day");        		
			JSONArray today = (JSONArray) near_earth_objects.get(start_date);
			Iterator<Object> iterator = today.iterator();
			while (iterator.hasNext()) {
				String color = "no danger";
			    JSONObject astroid = (JSONObject) iterator.next();
				JSONArray close_approach_data = (JSONArray) astroid.get("close_approach_data");
			    JSONObject a = close_approach_data.getJSONObject(0);
			    JSONObject relative_velocity = (JSONObject) a.get("relative_velocity");
				JSONObject estimated_diameter = (JSONObject) astroid.get("estimated_diameter");
				JSONObject meters = (JSONObject) estimated_diameter.get("meters");
				double estimated_diameter_min = (double) meters.get("estimated_diameter_min");
			    JSONObject miss_distance = (JSONObject) a.get("miss_distance");
			    
			    if (Double.parseDouble((String) relative_velocity.get("kilometers_per_second"))>=speed && (boolean)astroid.get("is_potentially_hazardous_asteroid")){
			    	color = "green";
					if (estimated_diameter_min>=diameter){
					    color = "yellow";
					    if (Double.parseDouble((String) miss_distance.get("astronomical"))>=miss){
					    	color = "red";
					    }
					}
				}
			    
		    	ans = ("ASTROID:"
			    + "NAME: "+astroid.get("name")
			    +" DATE: "+start_date
			    +" KPS: "+ relative_velocity.get("kilometers_per_second")
			    +" ED_MIN: "+ meters.get("estimated_diameter_min")
			    +" ED_MAX: "+ meters.get("estimated_diameter_max")
			    +" MD: "+ miss_distance.get("astronomical")
			    +" STATUS: "+ color
			    );
		    	sendans((String) astroid.get("name"));
			}
			if (start_date.equals(end_date))return;
			// add one day
			mydate = mydate.plusDays(1);
			start_date = mydate.toString();
			d=d-1;			
	    }
	}
    public static void sendans(String name){
        // Send a message
        System.out.println("WORKER - Sending a message - informing manager");        		
        SendMessageRequest send = new SendMessageRequest();
        send.withMessageBody(name);
        send.withQueueUrl(worker_manager);
        send.setMessageGroupId("messageGroup1");
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put("ans", new MessageAttributeValue().withDataType("String").withStringValue(ans));
        messageAttributes.put("local", new MessageAttributeValue().withDataType("String").withStringValue(local));
        send.withMessageAttributes(messageAttributes);
        sqs.sendMessage(send);
    }
    
    public static void finished(){
        // Send a message
        System.out.println("WORKER - finished job - informing manager");        		
        SendMessageRequest send = new SendMessageRequest();
        send.withMessageBody(myMessage+"finished");
        send.withQueueUrl(worker_manager);
        send.setMessageGroupId("messageGroup1");
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put("local", new MessageAttributeValue().withDataType("String").withStringValue(local));
        send.withMessageAttributes(messageAttributes);
        sqs.sendMessage(send);
    }
    
    public static String retrieveInstanceId() throws Exception 
    {
    String EC2Id = "";
    String inputLine;
    URL EC2MetaData = new URL("http://169.254.169.254/latest/meta-data/instance-id");
    URLConnection EC2MD = EC2MetaData.openConnection();
    BufferedReader in = new BufferedReader(
    new InputStreamReader(
    EC2MD.getInputStream()));
    while ((inputLine = in.readLine()) != null)
    {	
    EC2Id = inputLine;
    }
    in.close();
    return EC2Id;
    }
}