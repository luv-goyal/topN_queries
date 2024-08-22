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
        String dataUrl_ch2 = "http://10.83.46.213/admin/tenants";
        String dataUrl_hyd = "http://10.24.42.116/admin/tenants";

        String data_ch2 = readDataFromServer(dataUrl_ch2);
        String data_hyd = readDataFromServer(dataUrl_hyd);

        String[] tenants_ch2 = extractTenantId(data_ch2);
        String[] tenants_hyd = extractTenantId(data_hyd);

        String ip_ch2 = "10.83.46.213";
        String ip_hyd = "10.24.42.116";

        for (String tenant : tenants_ch2) {
            String jsonResponse = accessUrlWithTenantId(tenant.replace("\"", ""), ip_ch2);
            storeIntoDatabase(jsonResponse, tenant, "ch2");
        }

        for (String tenant : tenants_hyd) {
            String jsonResponse = accessUrlWithTenantId(tenant.replace("\"", ""), ip_hyd);
            storeIntoDatabase(jsonResponse, tenant, "hyd");
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


    public static String accessUrlWithTenantId(String tenantId, String ip) throws IOException, InterruptedException {

        String frontUrl = "http://" + ip;
        String urlWithTenantId = frontUrl + "/select/" + tenantId+ "/prometheus/api/v1/status/top_queries?topN=10&maxLifetime=1h" ;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(urlWithTenantId))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }


    public static void storeIntoDatabase(String jsonResponse, String tenant, String zone) {

        JSONObject jsonObject = new JSONObject(jsonResponse);

        String jdbcUrl = "jdbc:mysql://localhost:3306/topN";
        String username = "root";
        String password = "gieT6axo!@#%";

        insertIntoCountTable(jsonObject, jdbcUrl, username, password, tenant, zone);
        insertIntoAvgDurationTable(jsonObject, jdbcUrl, username, password, tenant, zone);
        insertIntoSumDurationTable(jsonObject, jdbcUrl, username, password, tenant, zone);
    }


    public static void insertIntoCountTable(JSONObject jsonObject, String jdbcUrl, String username, String password, String tenant, String zone) {
        JSONArray topByCountArray = jsonObject.getJSONArray("topByCount");
        String sqlCount = "INSERT INTO topByCount (query, query_time_interval, count, tenant_id, zone) VALUES (?, ?, ?, ?, ?)";

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
                pmtCount.setString(4, tenant);
                pmtCount.setString(5, zone);

                pmtCount.executeUpdate();
            }

        } catch (SQLException e) {
            System.out.println("Error inserting Count query data: " + e.getMessage());
        }

    }


    public static void insertIntoAvgDurationTable(JSONObject jsonObject, String jdbcUrl, String username, String password, String tenant, String zone) {
        JSONArray topByAvgDurationArray = jsonObject.getJSONArray("topByAvgDuration");
        String sqlAvgDuration = "INSERT INTO topByAvgDuration (query, query_time_interval, avg_duration, count, tenant_id, zone) VALUES (?, ?, ?, ?, ?, ?)";

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
                pmtAvgDuration.setString(5, tenant);
                pmtAvgDuration.setString(6, zone);

                pmtAvgDuration.executeUpdate();
            }
        } catch (SQLException e) {
            System.out.println("Error inserting Avg duration query data: " + e.getMessage());
        }

    }


    public static void insertIntoSumDurationTable(JSONObject jsonObject, String jdbcUrl, String username, String password, String tenant, String zone) {
        JSONArray topBySumDurationArray = jsonObject.getJSONArray("topBySumDuration");
        String sqlSumDuration = "INSERT INTO topBySumDuration (query, query_time_interval, sum_duration, count, tenant_id, zone) VALUES (?, ?, ?, ?, ?, ?)";

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
                pmtSumDuration.setString(5, tenant);
                pmtSumDuration.setString(6, zone);

                pmtSumDuration.executeUpdate();
            }
        } catch (SQLException e) {
            System.out.println("Error inserting Sum duration query data: " + e.getMessage());
        }
    }
}

