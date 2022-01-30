package com.example.demo.service;

import com.fasterxml.jackson.databind.jsonschema.JsonSchema;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.UUID;

@Service
public class InplaceShareService {
    public final MyJDBC myJDBC;

    public InplaceShareService(MyJDBC myJDBC) {

        this.myJDBC = myJDBC;
    }

    public void insertDatasets(String shareId, JSONArray datasets) {
        String sqlText;
        try {
            for (Object o : datasets) {
                UUID datasetId = UUID.randomUUID();
                JSONObject dataset = (JSONObject) o;
                sqlText = "INSERT INTO inplace_datasets (id, share_id, dataset_name, description, defination) Values (" +
                        "'" + datasetId + "'," +
                        "'" + shareId + "'," +
                        "'" + dataset.get("datasetName").toString() + "'," +
                        "'" + dataset.get("description").toString() + "'," +
                        "'" + dataset.get("defination").toString() + "')";

                Statement statement = myJDBC.getStatement("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
                statement.executeUpdate(sqlText);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertSubscribers(String shareId, Object subscriber) {

        JSONArray subscribers = new JSONArray();
        if (subscriber.getClass().getSimpleName().equals("JSONObject")) {
            subscribers.add(subscriber);
        } else {
            subscribers = (JSONArray) subscriber;
        }

        if (subscribers.isEmpty()) return;
        String sqlText;
        try {
            for (Object o : subscribers) {
                JSONObject s = (JSONObject) o;

                sqlText = "INSERT INTO inplace_subscribers (share_id, name, email) Values (" +
                        "'" + shareId + "'," +
                        "'" + s.get("name").toString() + "'," +
                        "'" + s.get("email").toString() + "')";

                Statement statement = myJDBC.getStatement("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
                statement.executeUpdate(sqlText);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public String deleteSubscriber(String shareId, String name) {
        String sqlText = "Delete from inplace_subscribers where share_id = '" + shareId + "'" + " and name = '" + name + "'";
        try {
            Statement statement = myJDBC.getStatement("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
            statement.executeUpdate(sqlText);
            return "subscriber deleted successfully";
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return "Failed to delete the subscriber";
    }
    public Object insertShares(String requestBody) {
         try {
             UUID shareId = UUID.randomUUID();
             JSONObject body = (JSONObject) new JSONParser().parse(requestBody);
             // Insert into share table
             String sqlText = "INSERT INTO inplace_share (id, name, direction, description) Values (" +
                     "'" +shareId+ "'," +
                     "'" +body.get("name").toString() + "'," +
                     "'" +body.get("direction").toString() + "'," +
                     "'" +body.get("description").toString() + "')";

             Statement statement = myJDBC.getStatement("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
             statement.executeUpdate(sqlText);

             // Insert Datasets
             this.insertDatasets(shareId.toString(), (JSONArray) body.get("datasets"));

             //Insert Subscribers
             this.insertSubscribers(shareId.toString(), body.get("subscribers"));
             return " Share "+shareId+" has been inserted";
         } catch ( ParseException| SQLException e) {
             e.printStackTrace();
         }
        return new JSONObject();
     }

    public JSONArray getShareDatasets(String shareId) {
        String sqlDataset = "Select * from inplace_datasets where share_id = '" + shareId + "'";
        return myJDBC.getData(sqlDataset);
    }

    public JSONArray getShares(String id) {
        String sqlText;
        try {

            if (id == null) {
                sqlText = " Select * from inplace_share";
                return myJDBC.getData(sqlText);
            }
            else {
                sqlText = "Select * from inplace_share where share_id = '" + id + "'";
                JSONArray result = myJDBC.getData(sqlText);

                // bring the datasets
                JSONObject datasets = new JSONObject();
                datasets.put("datasets", getShareDatasets(id));

                result.add(datasets);

                // bring the subscribers
                String sqlSubscriber = "Select * from inplace_subscribers where share_id = '" + id + "'";
                JSONObject subscribers = new JSONObject();
                subscribers.put("subscribers", myJDBC.getData(sqlSubscriber));

                result.add(subscribers);
                return result;
            }
        } catch ( Exception e) {
            e.printStackTrace();
        }
        return new JSONArray();
    }

    public void updateShare(String shareId, String requestBody) {
        if (requestBody.isEmpty()) return;
        try {
            JSONObject body = (JSONObject) new JSONParser().parse(requestBody);
            String sql = "Update inplace_share SET ";
            for (String s: body.keySet()) {
                sql += s + "='" + body.get(s).toString() + "',";
            }
            sql = sql.substring(0, sql.length()-1) +  " where id = '"+ shareId + "'";
            System.out.println(sql);
            Statement statement = myJDBC.getStatement("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
            statement.executeUpdate(sql);
        } catch (ParseException | SQLException e) {
            e.printStackTrace();
        }
    }

    public JSONObject getDatasetSchema(String shareId, String datasetName) {
        String sqlText = "select defination from inplace_datasets where share_id ='" +shareId+ "'" + "and dataset_name = '" +datasetName+ "'";
        try {
            JSONArray result = myJDBC.getData(sqlText);
            String defination = "";
            if ( !result.isEmpty()) {
                System.out.print(result);
                JSONObject jsonObject = (JSONObject) result.get(0);
                defination = defination + jsonObject.get("defination").toString();
            }
            Statement statement = myJDBC.getStatement("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
            ResultSet response = statement.executeQuery(defination);
            
            ResultSetMetaData metaData = response.getMetaData();
            JSONObject output = new JSONObject();
            JSONArray columns = new JSONArray();

            for (int i = 1; i<=metaData.getColumnCount(); i++) {
                JSONObject columnDetail = new JSONObject();
                columnDetail.put("name", metaData.getColumnName(i));
                columnDetail.put("datatype", metaData.getColumnTypeName(i));
                columns.add(columnDetail);
            }
            output.put("columns", columns);
            return output;
        } catch(Exception e) {
            e.printStackTrace();
        }
        return new JSONObject();
    }
}
