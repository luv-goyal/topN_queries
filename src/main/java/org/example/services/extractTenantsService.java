package org.example.services;

import org.json.JSONArray;
import org.json.JSONObject;

public class extractTenantsService {
    public static String[] extractTenantId(String data) {
        JSONObject jsonObject = new JSONObject(data);
        JSONArray jsonArray = jsonObject.getJSONArray("data");

        String[] tenants = new String[jsonArray.length()];
        for (int i = 0; i < jsonArray.length(); i++) {
            tenants[i] = jsonArray.getString(i);
        }

        return tenants;
    }
}
