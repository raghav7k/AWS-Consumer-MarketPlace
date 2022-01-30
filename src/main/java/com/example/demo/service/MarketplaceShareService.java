package com.example.demo.service;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.UUID;

@Service
public class MarketplaceShareService {

    private final MyJDBC myJDBC;

    public MarketplaceShareService(MyJDBC myJDBC) {

        this.myJDBC = myJDBC;
    }
    public String insertShares(JSONObject requestBody) {
        UUID shareId = UUID.randomUUID();
        String sqlText = "INSERT INTO marketplace_share (id, name, description, direction, remote_id) Values (" +
                "'" +shareId+ "'," +
                "'" +requestBody.get("name").toString() + "'," +
                "'" +requestBody.get("description").toString() + "'," +
                "'" +requestBody.get("direction").toString() + "'," +
                "'" +requestBody.get("remote_id").toString() + "')";
        System.out.println(sqlText);

        Statement statement = myJDBC.getStatement("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
        try {
            statement.executeUpdate(sqlText);
            return "Share " + shareId + " has been created";
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "failed to create the share";
    }

    public JSONArray getShares(String id) {
        String sqlText = " Select * from marketplace_share";
        if ( id != null) {
            sqlText = sqlText + " where id = '" + id+ "'";
        }
        try {
            JSONArray jsonArray = myJDBC.getData(sqlText);
            System.out.println("here");
            return jsonArray;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public JSONArray getShareDatasets(String shareId) {
        String sqlText = "select id, bucket, event_id from marketplace_datasets where share_id = '" +shareId+ "'";
        JSONArray result = myJDBC.getData(sqlText);
        return result;
    }

    public void insertDatasetsInShare(String shareId, String datasetId, String bucket, String event_id) {
        String sqlText;
        if ( event_id != null) {
            sqlText = "INSERT INTO marketplace_datasets (share_id, id, bucket, event_id) Values (" +
                    "'" +shareId+ "'," +
                    "'" +datasetId+ "'," +
                    "'" +bucket+ "'," +
                    "'" +event_id+ "')";
        } else {
            sqlText = "INSERT INTO marketplace_datasets (share_id, id, bucket) Values (" +
                    "'" + shareId + "'," +
                    "'" + datasetId + "'," +
                    "'" + bucket + "')";
        }
        Statement statement = myJDBC.getStatement("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
        try {
            statement.executeUpdate(sqlText);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertEventInDataset(String eventId, String datasetId, String shareId, String bucket) {
        String sqlText = "Select * from marketplace_datasets " +
                "where id = '" +datasetId+ "'" + " and share_id = '"+shareId+ "'";
        JSONArray result = myJDBC.getData(sqlText);
        if ( result.isEmpty()) {
            this.insertDatasetsInShare(shareId, datasetId, bucket, eventId);
        } else {
            sqlText = "Update marketplace_datasets SET event_id = '" +eventId+ "'" +
                    " where id = '" +datasetId+ "'" + " and share_id = '"+shareId+ "'";
            Statement statement = myJDBC.getStatement("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
            try {
                statement.executeUpdate(sqlText);
            } catch(SQLException e) {
                e.printStackTrace();
            }
        }
    }
    public void removeEventInDataset(String datasetId, String shareId) {
        String sqlText = "Update marketplace_datasets SET event_id = null" +
                " where id = '" +datasetId+ "'" + " and share_id = '"+shareId+ "'";
        Statement statement = myJDBC.getStatement("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
        try {
            statement.executeUpdate(sqlText);
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    public JSONObject getDatasetDetails(String datasetId, String shareId) {
        String sqlText = "Select * from  marketplace_datasets " +
                " where id = '" +datasetId+ "'" + " and share_id = '"+shareId+ "'";
        JSONArray result = myJDBC.getData(sqlText);
        System.out.println(result);
        if (!result.isEmpty()) {
            JSONObject datasetDetails = (JSONObject) result.get(0);
            return datasetDetails;
        } else {
            return new JSONObject();
        }

    }
}
