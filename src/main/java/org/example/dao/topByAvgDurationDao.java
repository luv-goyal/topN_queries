package org.example.dao;

import org.example.utils.dbutils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class topByAvgDurationDao {
    public static void insertIntoAvgDurationTable(JSONObject jsonObject, String tenant, String zone) {
        JSONArray topByAvgDurationArray = jsonObject.getJSONArray("topByAvgDuration");
        String sqlAvgDuration = "INSERT INTO topByAvgDuration (query, query_time_interval, avg_duration, count, tenant_id, zone) VALUES (?, ?, ?, ?, ?, ?)";

        try (Connection conn = dbutils.getConnection();
             PreparedStatement pmtAvgDuration = conn.prepareStatement(sqlAvgDuration)){

            for (int i = 0; i < topByAvgDurationArray.length(); i++) {
                JSONObject queryObject = topByAvgDurationArray.getJSONObject(i);

                String query = queryObject.getString("query");
                int query_time_interval = queryObject.getInt("timeRangeSeconds");
                int cnt = queryObject.getInt("count");
                double avgDuration = queryObject.getDouble("avgDurationSeconds");

                pmtAvgDuration.setString(1, query);
                pmtAvgDuration.setInt(2, query_time_interval);
                pmtAvgDuration.setInt(3, cnt);
                pmtAvgDuration.setDouble(4, avgDuration);
                pmtAvgDuration.setString(5, tenant);
                pmtAvgDuration.setString(6, zone);

                pmtAvgDuration.executeUpdate();
            }
        } catch (SQLException e) {
            System.out.println("Error inserting Avg Duration query data: " + e.getMessage());
        } finally {
            dbutils.closeConnection();
        }
    }
}
