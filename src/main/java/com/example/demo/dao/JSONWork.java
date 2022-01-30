package com.example.demo.dao;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONStyle;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.json.JSONException;
import org.springframework.stereotype.Repository;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Base64;
import java.util.UUID;


@Repository("JSON")
public class JSONWork {

    public UUID insertShares(String requestBody) throws JSONException {
        UUID shareId = UUID.randomUUID();
        try {
            JSONParser jsonParser = new JSONParser();
            JSONArray jsonArray = new JSONArray();
            try {
                FileReader file = new FileReader("myJson.json");
                Object obj = jsonParser.parse(file);
                jsonArray = (JSONArray) obj;
            } catch(FileNotFoundException | ClassCastException c) {

            }
            JSONObject jsonObject = (JSONObject) jsonParser.parse(requestBody);
            jsonObject.put("id", String.valueOf(shareId));
            jsonObject.put("datasets", new JSONArray());
            jsonArray.add(jsonObject);

            FileWriter fileWriter = new FileWriter("myJson.json");
            fileWriter.write(jsonArray.toString());
            fileWriter.flush();
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        return shareId;
    }

    public void insertDatasetInShare(String shareId, String datasetId, String bucketName) {
        Boolean override = false;
        try {
            FileReader fileReader = new FileReader("myJson.json");
            JSONParser jsonParser = new JSONParser();
            Object obj = jsonParser.parse(fileReader);
            JSONArray shares = (JSONArray) obj;

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id", datasetId);
            jsonObject.put("bucket_name", bucketName);

            JSONArray datasets = new JSONArray();
            for ( Object o: shares) {
                JSONObject share = (JSONObject) o;
                if ( share.get("id").toString().compareTo(shareId) == 0) {
                    datasets = (JSONArray) share.get("datasets");
                    datasets.add(jsonObject);
                    share.replace("datasets", datasets);
                    shares.remove(o);
                    shares.add(share);
                    override = true;
                    break;
                }
            }

            if ( override == true) {
                FileWriter fileWriter = new FileWriter("myJson.json");
                fileWriter.write(shares.toString());
                fileWriter.flush();
            }

        } catch (ParseException| IOException e) {
            e.printStackTrace();
        }
    }

    public JSONArray getShares(String id, String shareType) {
        String fileName = "myJson.json";
        if ( shareType != null && shareType.compareToIgnoreCase("inplace") == 0) {
            fileName = "inPlace.json";
        }
        JSONArray response = new JSONArray();
        try {
            JSONParser jsonParser = new JSONParser();
            FileReader file = new FileReader(fileName);
            Object obj = jsonParser.parse(file);
            JSONArray jsonArray = (JSONArray) obj;
            System.out.println(jsonArray);

            if ( id ==  null) return jsonArray;
            for (Object o : jsonArray) {
                JSONObject jsonObject = (JSONObject) o;
                if (jsonObject.get("id").toString().compareTo(id) == 0) {
                    response.add(jsonObject);
                    return response;
                }
            }
        } catch (FileNotFoundException | ClassCastException | ParseException e) {
        }
        return response;
    }

    public JSONArray getInPlaceDatasets(String id) throws FileNotFoundException {
        try {
            JSONParser jsonParser = new JSONParser();
            FileReader fileReader = new FileReader("inPlace.json");

            Object obj = jsonParser.parse(fileReader);
            JSONArray shares = (JSONArray) obj;
            JSONObject requiredShare = new JSONObject();

            for (Object share: shares) {
                JSONObject jsonObject = (JSONObject) share;
                if ( jsonObject.get("id").toString().compareToIgnoreCase(id) == 0) {
                    requiredShare = jsonObject;
                    break;
                }
            }
            if ( requiredShare.isEmpty()) {
                return new JSONArray();
            }
            return (JSONArray) requiredShare.get("datasets");
        } catch (FileNotFoundException | ClassCastException | ParseException e) {
        }

        return new JSONArray();
    }

    public String getSqlFromBody(String requestBody, String datasetName) throws ParseException {
        JSONObject body = (JSONObject) new JSONParser().parse(requestBody);
        String sql = "select ";
        JSONArray columns = (JSONArray) body.get("project");
        for ( Object o: (JSONArray) body.get("project")) {
            String column = (String) o;
            sql = sql + column + ",";
        }
        sql = sql.substring(0,sql.length()-1);
        JSONObject filter = (JSONObject) body.get("filter");
        sql = sql + " from " + datasetName+" where " + filter.getAsString("colname") + " ";

        switch (filter.getAsString("condition").toLowerCase()) {
            case "gt":
                sql = sql + "> ";
                break;

            case "lt":
                sql = sql + "< ";
                break;
            case "gte":
                sql = sql + ">= ";
                break;
            case "lte":
                sql = sql + "<= ";
                break;
            case "e":
                sql = sql + "= ";
                break;
        }
        sql = sql + "'" +filter.getAsString("value")+ "'";
        System.out.println(sql);
        return sql;
    }

    public String insertEventInDataset(String eventId, String datasetId, String shareId, String requestBody) {
        try {
            FileReader file = new FileReader("myJson.json");
            JSONArray shares = (JSONArray) new JSONParser().parse(file);
            JSONArray datasets = new JSONArray();
            JSONObject body = (JSONObject)new JSONParser().parse(requestBody);
            String bucket = (String) body.get("bucket");
            boolean autoUpdate = (boolean) body.get("autoUpdate");

            System.out.println(autoUpdate);
            JSONObject shared = new JSONObject();
            for ( Object o: shares) {
                JSONObject share = (JSONObject) o;
                if ( share.get("id").toString().compareToIgnoreCase(shareId) == 0) {
                    shares.remove(share);
                    shared = share;
                    datasets = (JSONArray) share.get("datasets");
                    break;
                }
            }

            if ( autoUpdate == true) {
                System.out.println("autoupdate is true");
                boolean found = false;
                if (!datasets.isEmpty()) {
                    for (Object o : datasets) {
                        JSONObject dataset = (JSONObject) o;
                        if (dataset.get("id").toString().compareToIgnoreCase(datasetId) == 0) {
                            found = true;
                            datasets.remove(dataset);
                            dataset.put("event_id", eventId);
                            datasets.add(dataset);
                            shared.replace("datasets", datasets);
                            shares.add(shared);
                            break;
                        }
                    }
                }

                if (found == false) {
                    JSONObject dataset = new JSONObject();
                    dataset.put("id", datasetId);
                    dataset.put("bucket", bucket);
                    dataset.put("event_id", eventId);
                    datasets.add(dataset);
                    shared.replace("datasets", datasets);
                    shares.add(shared);
                }
                FileWriter fileWriter = new FileWriter("myJson.json");
                fileWriter.write(shares.toString());
                fileWriter.flush();
            } else {
                for ( Object o: datasets) {
                    JSONObject dataset = (JSONObject) o;
                    if (dataset.get("id").toString().compareToIgnoreCase(datasetId) == 0) {
                        return dataset.get("event_id").toString();
                    }
                }
                return new String();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "done";
    }
}
