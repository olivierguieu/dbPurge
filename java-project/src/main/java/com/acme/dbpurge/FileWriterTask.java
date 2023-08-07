package com.acme.dbpurge;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWriterTask implements Runnable {
    private final String filePath;
    private final BlockingQueue<String> queue;
    private static Logger LOGGER = LoggerFactory.getLogger("FirstLogger");

    public FileWriterTask(String filePath, BlockingQueue<String> queue) {
        this.filePath = filePath;
        this.queue = queue;
    }

    @Override
    public void run() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath,true))) {
            while (true) {
                String line = queue.take(); // This will block if the queue is empty
                if ("EOF".equals(line)) {
                    break; // End the loop if EOF marker is found
                }
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException | InterruptedException e) {
            String stacktrace = ExceptionUtils.getStackTrace(e);
            LOGGER.error(stacktrace);
        }
    }
}