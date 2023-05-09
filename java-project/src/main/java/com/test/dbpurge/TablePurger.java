package com.test.dbpurge;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class TablePurger {

    private static final Logger LOGGER = LogManager.getLogger(TablePurger.class);

    private static final int BATCH_SIZE = 1000;

    private static final String DB_URL = "jdbc:mysql://localhost:3306/mydatabase";
    private static final String DB_USERNAME = "root";
    private static final String DB_PASSWORD = "password";

    private static final String ID_PROCESS_COLUMN_NAME = "id_process";
    private static final String ID_STOCK_COLUMN_NAME = "id_stock";

    public static List<TableToPurge> readTablesToPurgeFromYamlFile(String yamlFilePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final List<TableToPurge> tablesToPurge = new ArrayList<>();
        try (InputStream inputStream = new FileInputStream(yamlFilePath)) {
            Map<String, String> tables = mapper.readValue(inputStream, new TypeReference<Map<String, String>>() {
            });
            for (Map.Entry<String, String> entry : tables.entrySet()) {

                Optional<ColumnEnum> columnEnum = ColumnEnum.get(entry.getValue());
                if (columnEnum.isPresent()) {
                    tablesToPurge.add(new TableToPurge(entry.getKey(), columnEnum.get()));
                } else {
                    LOGGER.trace(
                            String.format("invalid column name {} for table {}", entry.getValue(), entry.getKey()));
                }
            }
        }
        return tablesToPurge;
    }

    public List<Integer> getListOfIdsFromTable(String tableName, ColumnEnum columnName) {
        List<Integer> list = new ArrayList<>();
        try (Connection conn = HikariCPDataSource.getConnection(); Statement stmt = conn.createStatement();) {
            String sql = String.format("SELECT DISTINCT %s FROM %s ORDER BY 1", columnName.getColumnName(),
                    tableName);
            System.out.println(sql);
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                // Display values
                int id = rs.getInt(columnName.getColumnName());
                System.out.print("ID: " + id);
                list.add(id);
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public List<Integer> getListOfIdProcessesToPurge(String tableName) {
        List<Integer> list = getListOfIdsFromTable(tableName, ColumnEnum.ID_PROCESS);
        return list;
    }

    public List<Integer> getListOfIdStocksToPurge(String tableName) {
        List<Integer> list = getListOfIdsFromTable(tableName, ColumnEnum.ID_STOCK);
        return list;
    }

    public void deleteRowsFromTableForOneId(String tableName, String columnName, Integer Id) {
        // Open a connection
        try (Connection conn = HikariCPDataSource.getConnection(); Statement stmt = conn.createStatement();) {
            String sql = String.format("DELETE FROM %s WHERE %s = %d ", tableName, columnName, Id);
            int count = stmt.executeUpdate(sql);
            LOGGER.trace(String.format("%d row(s) deleted from table %s on column %s", count, tableName, columnName));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteRowsFromTable(String tableName, ColumnEnum columnName, List<Integer> listIdToPurge) {
        listIdToPurge.parallelStream().forEach(id -> {
            // do the task with integer
            System.out
                    .println("Task with integer " + id + " is executed by thread " + Thread.currentThread().getName());
            deleteRowsFromTableForOneId(tableName, columnName.getColumnName(), id);
        });

    }

    public void run(String tablesDescription) {

        List<TableToPurge> tablesToPurge = null;
        try {
            tablesToPurge = readTablesToPurgeFromYamlFile(tablesDescription);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (tablesToPurge.isEmpty()) {
            LOGGER.info("No tables to purge.");
            return;
        }

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
            ExecutorService executorService = Executors.newFixedThreadPool(tablesToPurge.size());

            for (TableToPurge tableToPurge : tablesToPurge) {
                executorService.submit(() -> {
                    List<Integer> idsToPurge = new ArrayList<Integer>();
                    if (ID_PROCESS_COLUMN_NAME.equals(tableToPurge.columnEnum.getColumnName())) {
                        idsToPurge = getListOfIdProcessesToPurge(tableToPurge.tableName);
                    } else if (ID_STOCK_COLUMN_NAME.equals(tableToPurge.columnEnum.getColumnName())) {
                        idsToPurge = getListOfIdStocksToPurge(tableToPurge.tableName);
                    }
                    if (idsToPurge.size() > 0) {
                        LOGGER.info(String.format("Deleting {} rows from table '{}'...", idsToPurge.size(),
                                tableToPurge.tableName));
                        deleteRowsFromTable(tableToPurge.tableName, tableToPurge.columnEnum,
                                idsToPurge);
                        LOGGER.info(String.format("Deleted {} rows from table '{}'.", idsToPurge.size(),
                                tableToPurge.tableName));
                    }
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
        } catch (SQLException e) {
            LOGGER.error("Error connecting to database", e);
        }
    }
}
