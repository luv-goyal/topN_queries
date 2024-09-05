package org.example.services;

import java.io.IOException;

import static org.example.services.readDataService.readDataFromServer;

public class accessEndpointService {
    public static String accessUrlWithTenantId(String tenantId, String ip) throws IOException, InterruptedException {

        String frontUrl = "http://" + ip;
        String urlWithTenantId = frontUrl + "/select/" + tenantId+ "/prometheus/api/v1/status/top_queries?topN=10&maxLifetime=1h" ;

        return readDataFromServer(urlWithTenantId);
    }
}
