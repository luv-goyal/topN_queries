package org.example;

/**
 * Hello world!
 *
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;

public class App {
    public static void main(String[] args) throws IOException, InterruptedException {
        String dataUrl = "http://10.83.46.213/admin/tenants";
        String data = readDataFromServer(dataUrl);

        String[] tenants = extractTenantId(data);

        for (String tenant : tenants) {
            String jsonResponse = accessUrlWithTenantId(tenant.replace("\"", ""));
            storeIntoDatabase(jsonResponse);
        }
    }

    public static String readDataFromServer(String url) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    public static String[] extractTenantId(String data) {
        JSONObject jsonObject = new JSONObject(data);
        JSONArray jsonArray = jsonObject.getJSONArray("data");

        String[] tenants = new String[jsonArray.length()];
        for (int i = 0; i < jsonArray.length(); i++) {
            tenants[i] = jsonArray.getString(i);
        }

        return tenants;
    }

    public static String accessUrlWithTenantId(String tenantId) throws IOException, InterruptedException {
        String urlWithNumber = "http://10.83.46.213/select/" + tenantId+ "/prometheus/api/v1/status/top_queries?topN=10&maxLifetime=1h" ;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(urlWithNumber))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }

    public static void storeIntoDatabase(String jsonResponse) {

        JSONObject jsonObject = new JSONObject(jsonResponse);

        String jdbcUrl = "jdbc:mysql://localhost:3306/topN";
        String username = "root";
        String password = "gieT6axo!@#%";

        insertIntoCountTable(jsonObject, jdbcUrl, username, password);
        insertIntoAvgDurationTable(jsonObject, jdbcUrl, username, password);
        insertIntoSumDurationTable(jsonObject, jdbcUrl, username, password);
    }

    public static void insertIntoCountTable(JSONObject jsonObject, String jdbcUrl, String username, String password) {
        JSONArray topByCountArray = jsonObject.getJSONArray("topByCount");
        String sqlCount = "INSERT INTO topByCount (query, query_time_interval, count) VALUES (?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement pmtCount = conn.prepareStatement(sqlCount)) {
            for (int i = 0; i < topByCountArray.length(); i++) {
                JSONObject queryObject = topByCountArray.getJSONObject(i);

                String query = queryObject.getString("query");
                int query_time_interval = queryObject.getInt("timeRangeSeconds");
                int cnt = queryObject.getInt("count");

                pmtCount.setString(1, query);
                pmtCount.setInt(2, query_time_interval);
                pmtCount.setInt(3, cnt);

                pmtCount.executeUpdate();
            }

        } catch (SQLException e) {
            System.out.println("Error inserting Count query data: " + e.getMessage());
        }

    }

    public static void insertIntoAvgDurationTable(JSONObject jsonObject, String jdbcUrl, String username, String password) {
        JSONArray topByAvgDurationArray = jsonObject.getJSONArray("topByAvgDuration");
        String sqlAvgDuration = "INSERT INTO topByAvgDuration (query, query_time_interval, avg_duration, count) VALUES (?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement pmtAvgDuration = conn.prepareStatement(sqlAvgDuration)){

            for (int i = 0; i < topByAvgDurationArray.length(); i++) {
                JSONObject queryObject = topByAvgDurationArray.getJSONObject(i);

                String query = queryObject.getString("query");
                int query_time_interval = queryObject.getInt("timeRangeSeconds");
                int cnt = queryObject.getInt("count");
                Double avgDuration = queryObject.getDouble("avgDurationSeconds");

                pmtAvgDuration.setString(1, query);
                pmtAvgDuration.setInt(2, query_time_interval);
                pmtAvgDuration.setInt(3, cnt);
                pmtAvgDuration.setDouble(4, avgDuration);

                pmtAvgDuration.executeUpdate();
            }
        } catch (SQLException e) {
            System.out.println("Error inserting Avg duration query data: " + e.getMessage());
        }

    }

    public static void insertIntoSumDurationTable(JSONObject jsonObject, String jdbcUrl, String username, String password) {
        JSONArray topBySumDurationArray = jsonObject.getJSONArray("topBySumDuration");
        String sqlSumDuration = "INSERT INTO topBySumDuration (query, query_time_interval, sum_duration, count) VALUES (?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement pmtSumDuration = conn.prepareStatement(sqlSumDuration)){

            for (int i = 0; i < topBySumDurationArray.length(); i++) {
                JSONObject queryObject = topBySumDurationArray.getJSONObject(i);

                String query = queryObject.getString("query");
                int query_time_interval = queryObject.getInt("timeRangeSeconds");
                int cnt = queryObject.getInt("count");
                Double sumDuration = queryObject.getDouble("sumDurationSeconds");

                pmtSumDuration.setString(1, query);
                pmtSumDuration.setInt(2, query_time_interval);
                pmtSumDuration.setInt(3, cnt);
                pmtSumDuration.setDouble(4, sumDuration);

                pmtSumDuration.executeUpdate();
            }
        } catch (SQLException e) {
            System.out.println("Error inserting Sum duration query data: " + e.getMessage());
        }
    }
}

