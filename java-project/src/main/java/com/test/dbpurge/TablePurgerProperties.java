package com.test.dbpurge;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class TablePurgerProperties {
    
    private static TablePurgerProperties instance;

    private String dbUrl="";
    private String dbUser="";
    private String dbPassword="";
    

    private TablePurgerProperties(){}

    
    private void  loadProperties(String completePathToPropertyFile) {
        try (InputStream input = new FileInputStream(completePathToPropertyFile)) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            // get the property value and print it out
            dbUrl=prop.getProperty("db.url");
            dbUser=prop.getProperty("db.user");
            dbPassword=prop.getProperty("db.password");

            System.out.println(dbUrl);
            System.out.println(dbUser);
            System.out.println(dbPassword);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

  
    public static TablePurgerProperties getInstance(String  completePathToPropertyFile) {
        if (instance == null) {
            instance = new TablePurgerProperties();
            instance.loadProperties(completePathToPropertyFile);
        }
        return instance;
    }

}
