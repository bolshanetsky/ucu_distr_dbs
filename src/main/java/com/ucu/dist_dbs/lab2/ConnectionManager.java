package com.ucu.dist_dbs.lab2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Created by bolshanetskyi on 17.12.17.
 */
public class ConnectionManager {

    public static final String JDBC_CONNECTION = "jdbc:postgresql://localhost:5432/%s";

    public Connection getConnection(String dataBaseName) {
        Connection connection = null;
        Statement stmt = null;
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(String.format(JDBC_CONNECTION, dataBaseName),
                            "bolshanetskyi", "");
            System.out.println("Opened database connection successfully");

        } catch ( Exception e ) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }

        return connection;
    }
}
