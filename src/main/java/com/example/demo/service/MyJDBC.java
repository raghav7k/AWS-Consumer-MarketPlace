package com.example.demo.service;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;

@Service
public class MyJDBC {

    @Autowired
    public MyJDBC(){

    }
    public Statement getStatement(String url, String user, String password) {
        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            Statement statement = connection.createStatement();
            return statement;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public JSONArray getData(String sql) {
        try {
            Statement statement = this.getStatement("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
            ResultSet resultSet = statement.executeQuery(sql);

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnsNumber = resultSetMetaData.getColumnCount();

            JSONArray output = new JSONArray();
            while (resultSet.next()) {
                JSONObject jsonObject = new JSONObject();
                for (int i = 1; i <= columnsNumber; i++) {
                    if (resultSet.getString(i) != null)
                        jsonObject.put(resultSetMetaData.getColumnName(i), resultSet.getString(i));
                }
                output.add(jsonObject);
            }
            return output;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new JSONArray();
    }


    public JSONArray getDataset(String sql) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        int columnCount = resultSetMetaData.getColumnCount();
        JSONArray response = new JSONArray();
        while (resultSet.next()) {
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                jsonObject.put(resultSetMetaData.getColumnName(i), resultSet.getString(i));
            }
            response.add(jsonObject);
        }
        return response;
    }

    public JSONObject getDatasetSchema(String datasetName) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
        Statement statement = connection.createStatement();
        String sql = "Select * from "+ datasetName;
        ResultSet response = statement.executeQuery(sql);

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
    }

    public String getDatasetSQl(String shareId, String datasetName) {
        try {
            String sqlText = "select defination from inplace_datasets where share_id = '" +shareId+ "'" + " and dataset_name= '"+datasetName+ "'";
            JSONArray result = this.getData(sqlText);
            JSONObject dataset = (JSONObject) result.get(0);
            String sql = dataset.get("defination").toString();
            sql = "("+sql+") as data";
            return sql;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public JSONObject getDatasetData(String sql) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
        Statement statement = connection.createStatement();
        ResultSet response = statement.executeQuery(sql);

        JSONObject output = new JSONObject();

        JSONArray data = new JSONArray();
        ResultSetMetaData resultSetMetaData = response.getMetaData();

        int columnCount = resultSetMetaData.getColumnCount();
        int rows = 0;
        while (response.next()) {
            rows++;
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                jsonObject.put(resultSetMetaData.getColumnName(i), response.getString(i));
            }
            data.add(jsonObject);
        }
        output.put("numrows", rows);
        output.put("data", data);
        return output;
    }


    public void createView(String name, String view) {
        try {
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/jdbc-share", "root", "Raghav@123#");
            Statement statement = connection.createStatement();
            String sql = "Create view "+name+ " as "+view;
            statement.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
