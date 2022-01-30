package com.example.demo.dao;

import com.example.demo.service.MyJDBC;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.iot.model.MessageFormat;
import software.amazon.awssdk.services.iot.model.SnsAction;
import software.amazon.awssdk.services.ses.model.SNSAction;
import software.amazon.awssdk.services.ses.model.SNSDestination;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

@Repository("InPlace")
public class InPlaceWork {

    public final JSONWork jsonWork;
    private final MyJDBC myJDBC;

    @Autowired
    public InPlaceWork(@Qualifier("JSON") JSONWork jsonWork, MyJDBC myJDBC) {
        this.jsonWork = jsonWork;
        this.myJDBC = myJDBC;
    }
    public Object inPlaceShare(String requestBody) throws ParseException {
        String share_id = UUID.randomUUID().toString();
        JSONParser jsonParser = new JSONParser();
        JSONArray fileArray = new JSONArray();
        JSONObject fileObject = new JSONObject();
        // Read the file and fetch the existing array.
        try {
            FileReader fileReader = new FileReader("inPlace.json");
            Object obj = jsonParser.parse(fileReader);
            fileArray = (JSONArray) obj;
        } catch (FileNotFoundException | ClassCastException e) {
        }


        JSONObject body = (JSONObject) jsonParser.parse(requestBody);
        JSONObject response = new JSONObject();
        response.put("id", share_id);
        response.put("name", body.get("name"));
        JSONArray datasetsInBody = (JSONArray) body.get("datasets");


        JSONArray datasetsInFile = new JSONArray();

        for(Object o: datasetsInBody) {
            JSONObject jo = (JSONObject) o;
            String dataSetId = UUID.randomUUID().toString();

            myJDBC.createView(jo.get("datasetName").toString(),jo.get("defination").toString());
            jo.put("id", dataSetId);
            jo.get("description");
            jo.get("datasetName");
            datasetsInFile.add(jo);
        }

        response.put("datasets", datasetsInFile);

        fileObject.put("name", body.get("name"));
        fileObject.put("id", share_id);
        fileObject.put("model", body.get("model"));
        fileObject.put("description", body.get("description"));
        fileObject.put("subscribers", body.get("subscribers"));
        fileObject.put("datasets", datasetsInFile);
        fileArray.add(fileObject);

        System.out.println(fileArray);

        try {
            FileWriter fileWriter = new FileWriter("inPlace.json");
            fileWriter.write(fileArray.toJSONString());
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return response;
    }

    public JSONObject updateShare(String shareId, String requestBody) throws ParseException, IOException {
        JSONArray shares = jsonWork.getShares(null, "inplace");
        JSONObject share = (JSONObject) jsonWork.getShares(shareId, "inplace").get(0);
        shares.remove(share);
        JSONObject body = (JSONObject) new JSONParser().parse(requestBody);
        for ( String s : body.keySet()) {
            share.replace(s, body.get(s));
        }
        shares.add(share);

        FileWriter  fileWriter = new FileWriter("inPlace.json");
        fileWriter.write(shares.toString());
        fileWriter.flush();

        return share;
    }

    public JSONObject addSubscribers(String shareId, String requestBody) throws IOException, ParseException {
        JSONArray shares = jsonWork.getShares(null, "inplace");
        JSONObject share = (JSONObject) jsonWork.getShares(shareId, "inplace").get(0);
        shares.remove(share);
        JSONArray subscribers = (JSONArray) share.get("subscribers");
        subscribers.add(new JSONParser().parse(requestBody));
        share.replace("subscribers", subscribers);
        shares.add(share);

        FileWriter  fileWriter = new FileWriter("inPlace.json");
        fileWriter.write(shares.toString());
        fileWriter.flush();
        return share;
    }

    public JSONObject deleteSubscriber(String shareId, String name) throws IOException {
        JSONArray shares = jsonWork.getShares(null, "inplace");
        JSONObject share = (JSONObject) jsonWork.getShares(shareId, "inplace").get(0);
        shares.remove(share);
        JSONArray subscribers = (JSONArray) share.get("subscribers");

        for (Object o: subscribers) {
            JSONObject subscriber = (JSONObject) o;
            if ( subscriber.get("name").toString().compareToIgnoreCase(name) == 0) {
                subscribers.remove(subscriber);
                share.replace("subcribers", subscribers);
                shares.add(share);
                break;
            }
        }
        FileWriter  fileWriter = new FileWriter("inPlace.json");
        fileWriter.write(shares.toString());
        fileWriter.flush();
        return share;
    }

    public void sendInvitation(String name, JSONObject share) {
        JSONArray subscribers = (JSONArray) share.get("subscribers");
        String email;
        for ( Object o: subscribers) {
            JSONObject subscriber = (JSONObject) o;
            if ( subscriber.get("name").toString().compareToIgnoreCase(name) == 0) {
                email = subscriber.get("email").toString();
                break;
            }
        }

        String topicARN = "arn:aws:sns:us-east-1:844102931058:"+ name;
        PublishRequest publishRequest = PublishRequest.builder().message("Invitation for the share " + share.get("name")).
                subject("Invitation of share object").topicArn(topicARN).build();
        SnsClient snsClient = SnsClient.create();
        PublishResponse publishResponse = snsClient.publish(publishRequest);
    }

}
