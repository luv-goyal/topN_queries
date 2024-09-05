package org.example.application;

/**
 * Hello world!
 *
 */

import java.io.IOException;

import static org.example.services.DatabaseStoringService.storeCh2TenantsQueries;
import static org.example.services.DatabaseStoringService.storeHydTenantsQueries;

public class App {
    public static void main(String[] args) throws IOException, InterruptedException {
        storeCh2TenantsQueries();
        storeHydTenantsQueries();
    }
}

