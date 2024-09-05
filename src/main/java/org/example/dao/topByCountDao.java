package org.example.dao;

import org.example.utils.dbutils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class topByCountDao {
    public static void insertIntoCountTable(JSONObject jsonObject, String tenant, String zone) {
        JSONArray topByCountArray = jsonObject.getJSONArray("topByCount");
        String sqlCount = "INSERT INTO topByCount (query, query_time_interval, count, tenant_id, zone) VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = dbutils.getConnection();
             PreparedStatement pmtCount = conn.prepareStatement(sqlCount)) {
            for (int i = 0; i < topByCountArray.length(); i++) {
                JSONObject queryObject = topByCountArray.getJSONObject(i);

                String query = queryObject.getString("query");
                int query_time_interval = queryObject.getInt("timeRangeSeconds");
                int cnt = queryObject.getInt("count");

                pmtCount.setString(1, query);
                pmtCount.setInt(2, query_time_interval);
                pmtCount.setInt(3, cnt);
                pmtCount.setString(4, tenant);
                pmtCount.setString(5, zone);

                pmtCount.executeUpdate();
            }

        } catch (SQLException e) {
            System.out.println("Error inserting Count query data: " + e.getMessage());
        } finally {
            dbutils.closeConnection();
        }
    }
}
