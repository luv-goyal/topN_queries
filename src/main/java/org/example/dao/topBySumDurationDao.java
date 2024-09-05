package org.example.dao;

import org.example.utils.dbutils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class topBySumDurationDao {
    public static void insertIntoSumDurationTable(JSONObject jsonObject, String tenant, String zone) {
        JSONArray topBySumDurationArray = jsonObject.getJSONArray("topBySumDuration");
        String sqlSumDuration = "INSERT INTO topBySumDuration (query, query_time_interval, sum_duration, count, tenant_id, zone) VALUES (?, ?, ?, ?, ?, ?)";

        try (Connection conn = dbutils.getConnection();
             PreparedStatement pmtSumDuration = conn.prepareStatement(sqlSumDuration)){

            for (int i = 0; i < topBySumDurationArray.length(); i++) {
                JSONObject queryObject = topBySumDurationArray.getJSONObject(i);

                String query = queryObject.getString("query");
                int query_time_interval = queryObject.getInt("timeRangeSeconds");
                int cnt = queryObject.getInt("count");
                double sumDuration = queryObject.getDouble("sumDurationSeconds");

                pmtSumDuration.setString(1, query);
                pmtSumDuration.setInt(2, query_time_interval);
                pmtSumDuration.setInt(3, cnt);
                pmtSumDuration.setDouble(4, sumDuration);
                pmtSumDuration.setString(5, tenant);
                pmtSumDuration.setString(6, zone);

                pmtSumDuration.executeUpdate();
            }
        } catch (SQLException e) {
            System.out.println("Error inserting Sum duration query data: " + e.getMessage());
        } finally {
            dbutils.closeConnection();
        }
    }
}
