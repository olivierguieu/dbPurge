package com.acme.dbpurge;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TablePurger {

    private static Logger LOGGER = LoggerFactory.getLogger("FirstLogger");

    static public void run(List<TableToPurge> tablesToPurge) {
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(tablesToPurge.size());

            for (TableToPurge tableToPurge : tablesToPurge) {
                executorService.submit(() -> {
                    tableToPurge.purgeTable();
                });
            }

            executorService.shutdown();
            while (!executorService.isTerminated()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.error("Thread interrupted while waiting for executor service to terminate", e);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (Exception e) {
            String stacktrace = ExceptionUtils.getStackTrace(e);
            LOGGER.error(stacktrace);
        }
    }
}
