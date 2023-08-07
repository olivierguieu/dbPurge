package com.acme.dbpurge;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    private static Logger LOGGER = LoggerFactory.getLogger("FirstLogger");
    private static PurgerLogger purgerLogger = null;
    
    private static InputStream getFileAsIOStream(final String fileName) {
        InputStream ioStream = App.class.getClassLoader().getResourceAsStream(fileName);

        if (ioStream == null) {
            throw new IllegalArgumentException(fileName + " is not found");
        }
        return ioStream;
    }

    public static boolean main(String[] args) {
        boolean bRes = false;
        DatabasePurgerProperties.getInstance("src/main/resources/config.properties");

        List<TableToPurge> tablesToPurge = null;
        purgerLogger = new PurgerLogger();
        try {

            tablesToPurge = Utils.readTablesToPurgeFromYamlFile(getFileAsIOStream("tables_to_purge.yaml"),
                    purgerLogger);

            if (tablesToPurge.isEmpty()) {
                LOGGER.info("No tables to purge.");
            } else {
                purgerLogger.init();
                TablePurger.run(tablesToPurge);
                purgerLogger.stop();

                bRes = true;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return bRes;
    }
}
