import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.URL;
import java.text.SimpleDateFormat;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class local{
	
	public static PropertiesCredentials Credentials;
	public static AmazonS3 S3;
	public static String bucketName = "ishay";
	public static String propertiesFilePath = "src/ishay.properties";
	public static AmazonSQSClient sqs;
	public static AmazonEC2 ec2;

	public static int n;
	public static int d;
	
	public static String manager_local = "https://sqs.us-east-2.amazonaws.com/605668889946/manager-local.fifo";
	public static String local_manager = "https://sqs.us-east-2.amazonaws.com/605668889946/local-manager.fifo";

	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException{

		//args
        System.out.println("LOCAL - Parsing args\n");        	    
		File input = new File("local/"+args[0]);
		File output = new File("local/"+args[1]);
		n = Integer.parseInt(args[2]);
		d = Integer.parseInt(args[3]);	
		if (d>7 || d<1 || n<1) return;
		
		//get name
        System.out.println("LOCAL - Getting unique name\n");        	    		
		URL whatismyip = new URL("http://checkip.amazonaws.com");
		BufferedReader in = new BufferedReader(new InputStreamReader(whatismyip.openStream()));
		String ip = in.readLine(); //you get the IP as a String
	    InetAddress addr = InetAddress.getLocalHost();
	    String hostname = addr.getHostName();
	    String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
	    System.out.println("NOW: " +timeStamp);
	    //connect to server
        System.out.println("LOCAL - connecting to servers\n");        	    		
		Credentials = new PropertiesCredentials(local.class.getResourceAsStream("ishay.properties"));
		S3 = new AmazonS3Client(Credentials);
        sqs = new AmazonSQSClient(Credentials);
        sqs.setEndpoint("https://sqs.us-east-2.amazonaws.com");       
        ec2 = new AmazonEC2Client(Credentials);

        //wakeup manager
        DescribeInstancesRequest request = new DescribeInstancesRequest();

        List<String> valuesT1 = new ArrayList<String>();
        valuesT1.add("manager");
        Filter filter1 = new Filter("tag:Name", valuesT1);
        DescribeInstancesResult result = ec2.describeInstances(request.withFilters(filter1));
        List<Reservation> reservations = result.getReservations();

        check:{
        for (Reservation reservation : reservations) {
        	List<Instance> instances = reservation.getInstances();
        	for (@SuppressWarnings("unused") Instance instance : instances) {
                System.out.println("LOCAL - manager already alive!!\n");        	    		
        		break check;
        	}
        }
        createManager();
        }

        //upload file
        System.out.println("LOCAL - upload input file to s3\n");        	    
		PutObjectRequest por = new PutObjectRequest(bucketName, ip+hostname+timeStamp ,input);
		por.withCannedAcl(CannedAccessControlList.PublicRead);
		S3.putObject(por);
		
        // Send a message
        System.out.println("LOCAL - Sending a message - informing manager\n");        		
        SendMessageRequest send = new SendMessageRequest();
        send.withMessageBody("request"+ip+hostname+timeStamp);
        send.withQueueUrl(local_manager);
        send.setMessageGroupId("messageGroup1");
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put("AccountId", new MessageAttributeValue().withDataType("String").withStringValue(ip+hostname+timeStamp));
        messageAttributes.put("n", new MessageAttributeValue().withDataType("String").withStringValue(""+n+""));
        messageAttributes.put("d", new MessageAttributeValue().withDataType("String").withStringValue(""+d+""));
        send.withMessageAttributes(messageAttributes);
        sqs.sendMessage(send);
          		
        SendMessageResult sendMessageResult = sqs.sendMessage(send);
        String sequenceNumber = sendMessageResult.getSequenceNumber();
        String messageId = sendMessageResult.getMessageId();
        System.out.println("SendMessage succeed with messageId " + messageId + ", sequence number " + sequenceNumber + "\n");

        // Receive messages
        System.out.println("LOCAL - waiting for message from manager\n");        		
        ReceiveMessageRequest recive = new ReceiveMessageRequest();
        recive.withQueueUrl(manager_local);
        boolean found = false;
        while (!found){
            List<Message> messages = sqs.receiveMessage(recive).getMessages();
            for (Message message : messages) {
            	if (message.getBody().equals(ip+hostname+timeStamp)){
            		// Delete the message
                    System.out.println("LOCAL - got message from manager\n");  
                    System.out.println("LOCAL - deleting message from manager\n");        		
            		String messageReceiptHandle = message.getReceiptHandle();
                  	sqs.deleteMessage(new DeleteMessageRequest(manager_local, messageReceiptHandle));
                  	found=true;
            	}
            }        	
        }
        
		//download result
        System.out.println("LOCAL - downloading output from s3\n");        		
        S3.getObject(new GetObjectRequest(bucketName, ip+hostname+timeStamp), output);
        if (output.exists() && output.canRead()){
    		System.out.println("File downloaded.");        	
        }
        
        //create html
        try {
            //define a HTML String Builder
            StringBuilder htmlStringBuilder=new StringBuilder();
            //append html header and title
            htmlStringBuilder.append("<html><head><title>ASTROIDS</title></head>");
            //append body
            htmlStringBuilder.append("<body>");
            //append table
            htmlStringBuilder.append("<table border=\"1\" bordercolor=\"#000000\">");
            JsonReader reader = new JsonReader();
    	    JSONObject json = reader.readJsonFromUrl("https://s3.amazonaws.com/ishay/"+ip+hostname+timeStamp);
			JSONArray arr = (JSONArray) json.get("list"); 
			Iterator<Object> iterator = arr.iterator();
			int i=1;
			while (iterator.hasNext()) {
			    String astroid = (String) iterator.next();
			    if (astroid.toString().contains("green")){
		            htmlStringBuilder.append("<tr bgcolor=\"green\"><td>"+i+++"</td><td>"+astroid.toString()+"</td></tr>");
			    }else if (astroid.toString().contains("yellow")){
		            htmlStringBuilder.append("<tr bgcolor=\"yellow\"><td>"+i+++"</td><td>"+astroid.toString()+"</td></tr>");
			    }else if(astroid.toString().contains("red")){
		            htmlStringBuilder.append("<tr bgcolor=\"red\"><td>"+i+++"</td><td>"+astroid.toString()+"</td></tr>");
			    }else{
		            htmlStringBuilder.append("<tr><td>"+i+++"</td><td>"+astroid.toString()+"</td></tr>");
			    }
			}
            //close html file
            htmlStringBuilder.append("</table></body></html>");
            File file = new File("local/index.html");
            //write to file with OutputStreamWriter
            OutputStream outputStream = new FileOutputStream(file.getAbsoluteFile());
            Writer writer=new OutputStreamWriter(outputStream);
            writer.write(htmlStringBuilder.toString());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
		//deleting result
        System.out.println("LOCAL - deleting output from s3\n");        		       
        S3.deleteObject(new DeleteObjectRequest(bucketName, ip+hostname+timeStamp));
        
	    String timeStamp2 = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
	    System.out.println("NOW: " +timeStamp2);

        if(args.length>4){
            if (args[4]!=null && args[4].equals("terminate")){
            	deleteManager();
            }
        }
    }
	
	public static void createManager(){
        System.out.println("LOCAL - creating manager\n");        		       
        RunInstancesRequest request = new RunInstancesRequest();
        request.setInstanceType(InstanceType.T2Micro.toString());
        request.setMinCount(1);
        request.setMaxCount(1);
        request.setImageId("ami-b66ed3de");
        request.setKeyName("ishay");
        request.setUserData(getUserDataScript());       
        RunInstancesResult runInstances = ec2.runInstances(request);
        
     // TAG EC2 INSTANCES
        List<Instance> instances = runInstances.getReservation().getInstances();
        for (Instance instance : instances) {
          CreateTagsRequest createTagsRequest = new CreateTagsRequest();
          createTagsRequest.withResources(instance.getInstanceId())
              .withTags(new Tag("Name", "manager"));
          ec2.createTags(createTagsRequest);
        }
	}
	
	public static void deleteManager(){
		// Send a message
        System.out.println("LOCAL - sending relese message for manager\n");        		
        SendMessageRequest relese = new SendMessageRequest();
        relese.withMessageBody("goToSleep");
        relese.withQueueUrl(local_manager);
        relese.setMessageGroupId("messageGroup1");
        sqs.sendMessage(relese);
	}

	public static String getUserDataScript(){
        ArrayList<String> lines = new ArrayList<String>();
        lines.add("#! /bin/bash");
        lines.add("wget https://s3.amazonaws.com/ishay/manager.zip -O manager.zip");
        lines.add("unzip -P 12661266 manager.zip");
        lines.add("java -jar manager.jar"); 
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

}
