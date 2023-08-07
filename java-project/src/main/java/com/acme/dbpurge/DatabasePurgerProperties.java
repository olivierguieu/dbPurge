package com.acme.dbpurge;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class DatabasePurgerProperties {

    private static DatabasePurgerProperties instance;

    private String dbUrl = "";
    private String dbUser = "";
    private String dbPassword = "";
    private Integer dbMaxIdToPurgeByTable = -1;

    private DatabasePurgerProperties() {
    }

    private void loadProperties(String completePathToPropertyFile) {
        try (InputStream input = new FileInputStream(completePathToPropertyFile)) {

            Properties prop = new Properties();

            prop.load(input);

            dbUrl = prop.getProperty("db.url");
            dbUser = prop.getProperty("db.user");
            dbPassword = prop.getProperty("db.password");

            String maxIdToPurgeByTable = prop.getProperty("db.max_id_to_purge_by_table");

            if (maxIdToPurgeByTable != null && maxIdToPurgeByTable.isEmpty() == false) {
                try {
                    dbMaxIdToPurgeByTable = Integer.parseInt(maxIdToPurgeByTable);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        display();
    }

    public static DatabasePurgerProperties getInstance(String completePathToPropertyFile) {
        if (instance == null) {
            instance = new DatabasePurgerProperties();
            instance.loadProperties(completePathToPropertyFile);
        }
        return instance;
    }

    public void display() {
        System.out.println("dburl=" + dbUrl);
        System.out.println("dbUser=" + dbUser);
        System.out.println("dbPassword=" + dbPassword);

        System.out.println("db.max_id_to_purge_by_table=" + dbMaxIdToPurgeByTable);
    }
}
