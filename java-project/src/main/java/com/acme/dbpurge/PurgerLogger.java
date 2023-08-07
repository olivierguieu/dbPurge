package com.acme.dbpurge;

import java.text.SimpleDateFormat;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PurgerLogger {
    private static Logger LOGGER = LoggerFactory.getLogger("FirstLogger");

    private final String FILE_PATH = "output.txt";
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();

    private boolean init = false;

    synchronized void init() {
        if (init) {
            return;
        }

        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
        UUID uuid = UUID.randomUUID();

        Thread writerThread = new Thread(
                new FileWriterTask(FILE_PATH + "_" + timeStamp + "_" + uuid.toString(), queue));
        writerThread.start();

        init = true;
    }

    void stop() {
        try {
            queue.put("EOF");
        } catch (InterruptedException e) {
            String stacktrace = ExceptionUtils.getStackTrace(e);
            LOGGER.error(stacktrace);
        }
    }

    void addToLog(String sql) {

        init();

        LOGGER.info(sql);
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());

        try {
            queue.put(timeStamp + "  " + sql);
        } catch (InterruptedException e) {
            String stacktrace = ExceptionUtils.getStackTrace(e);
            LOGGER.error(stacktrace);
        }
    }
}
