package org.example.services;

import org.json.JSONObject;

import java.io.IOException;

import static org.example.dao.topByAvgDurationDao.insertIntoAvgDurationTable;
import static org.example.dao.topByCountDao.insertIntoCountTable;
import static org.example.dao.topBySumDurationDao.insertIntoSumDurationTable;
import static org.example.services.accessEndpointService.accessUrlWithTenantId;
import static org.example.services.extractTenantsService.extractTenantId;
import static org.example.services.readDataService.readDataFromServer;

public class DatabaseStoringService {
    public static void storeIntoDatabase(String jsonResponse, String tenant, String zone) {

        JSONObject jsonObject = new JSONObject(jsonResponse);

        insertIntoCountTable(jsonObject, tenant, zone);
        insertIntoAvgDurationTable(jsonObject, tenant, zone);
        insertIntoSumDurationTable(jsonObject, tenant, zone);
    }

    public static void storeCh2TenantsQueries() throws IOException, InterruptedException {
        String dataUrl_ch2 = "http://10.83.46.213/admin/tenants";
        String data_ch2 = readDataFromServer(dataUrl_ch2);
        String[] tenants_ch2 = extractTenantId(data_ch2);
        String ip_ch2 = "10.83.46.213";

        for (String tenant : tenants_ch2) {
            String jsonResponse = accessUrlWithTenantId(tenant.replace("\"", ""), ip_ch2);
            storeIntoDatabase(jsonResponse, tenant, "ch2");
        }
    }

    public static void storeHydTenantsQueries() throws IOException, InterruptedException {
        String dataUrl_hyd = "http://10.24.42.116/admin/tenants";
        String data_hyd = readDataFromServer(dataUrl_hyd);
        String[] tenants_hyd = extractTenantId(data_hyd);
        String ip_ch2 = "10.24.42.116";

        for (String tenant : tenants_hyd) {
            String jsonResponse = accessUrlWithTenantId(tenant.replace("\"", ""), ip_ch2);
            storeIntoDatabase(jsonResponse, tenant, "hyd");
        }
    }
}
