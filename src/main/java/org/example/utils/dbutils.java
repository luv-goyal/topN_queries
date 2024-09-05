package org.example.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class dbutils {
    private static Connection connection = null;
    private static final String jdbcUrl = "jdbc:mysql://localhost:3306/topN";
    private static final String username = "root";
    private static final String password = "gieT6axo!@#%";
    public static Connection getConnection() throws SQLException {
        return connection = DriverManager.getConnection(jdbcUrl, username, password);
    }

    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
