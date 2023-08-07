package com.acme.dbpurge;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.yaml.snakeyaml.Yaml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static Logger LOGGER = LoggerFactory.getLogger("FirstLogger");

    public static List<TableToPurge> readTablesToPurgeFromYamlFile(InputStream inputStream, PurgerLogger purgerLogger)
            throws IOException {
        Yaml yaml = new Yaml();

        List<TableToPurge> tablesToPurge = new ArrayList<>();

        // Read YAML file into a List of Maps
        List<Map<String, Object>> msgDefinition = yaml.load(inputStream);

        // Iterate over the entries
        for (Map<String, Object> entry : msgDefinition) {
            int table = (int) entry.get("table");
            String name = (String) entry.get("name");
            String type = (String) entry.get("type");
            type = type.toUpperCase();

            ColumnEnum enumValue;
            try {
                // Consider using trim to eliminate extraneous whitespace
                enumValue = ColumnEnum.valueOf(type.trim());
            } catch (Exception e) {
                // handle the situation here. Here are a couple of ideas.
                // Apply null and expect the using code to detect.
                enumValue = null;
                // Have a defined private constant for a default value
                // assuming a default value would make more sense than null
                enumValue = ColumnEnum.ID_STOCK;
            }

            String selectSql = (String) entry.get("selectsql");
            String deleteSql = (String) entry.get("deletesql");

            tablesToPurge.add(new TableToPurge(name, enumValue, selectSql, deleteSql, purgerLogger));

            // Print the entry details
            LOGGER.info("Table: " + table);
            LOGGER.info("Name: " + name);
            LOGGER.info("Type: " + type);
        }

        return tablesToPurge;
    }

    public static List<TableToPurge> readTablesToPurgeFromYamlFile(String yamlFilePath, PurgerLogger purgerLogger)
            throws IOException {

        List<TableToPurge> tablesToPurge = new ArrayList<>();
        try (InputStream inputStream = new FileInputStream(yamlFilePath)) {
            tablesToPurge = readTablesToPurgeFromYamlFile(inputStream, purgerLogger);
        }
        return tablesToPurge;
    }

    public static void deleteRowsFromTable(String tableName, ColumnEnum columnName, Set<Integer> listIdToPurge,
            PurgerLogger purgerLogger) {
        listIdToPurge.parallelStream().forEach(id -> {
            LOGGER.info("Task with integer " + id + " is executed by thread " + Thread.currentThread().getName());
            deleteRowsFromTableForOneId(tableName, columnName.getColumnName(), id, purgerLogger);
        });
    }

    public static void deleteRowsFromTableForOneId(String tableName, String columnName, Integer Id,
            PurgerLogger purgerLogger) {
        String sql = String.format("DELETE FROM %s WHERE %s = %d ", tableName, columnName, Id);
        deleteRowsUsingDeleteQueryForOneId(sql, Id, purgerLogger);
    }

    public static void deleteRowsUsingDeleteQueryForOneId(String deleteQuery, Integer Id, PurgerLogger purgerLogger) {
        try (Connection conn = HikariCPDataSource.getConnection(); Statement stmt = conn.createStatement();) {
            String sql = String.format(deleteQuery, Id);
            int count = stmt.executeUpdate(sql);
            LOGGER.info(String.format("%d row(s) deleted from deleteQuery[%s]", count, deleteQuery));
            purgerLogger.addToLog(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void deleteRowsUsingDeleteQuery(String deleteQuery, Set<Integer> listIdToPurge,
            PurgerLogger purgerLogger) {
        listIdToPurge.parallelStream().forEach(id -> {
            LOGGER.info("Task with integer " + id + " is executed by thread " + Thread.currentThread().getName());
            deleteRowsUsingDeleteQueryForOneId(deleteQuery, id, purgerLogger);
        });
    }

    public static Set<Integer> getListOfIdInTable(String tableName, ColumnEnum columnName, String strFullWhereClause) {
        Set<Integer> list = new HashSet();
        try (Connection conn = HikariCPDataSource.getConnection(); Statement stmt = conn.createStatement();) {
            String sql = String.format("SELECT DISTINCT %s FROM %s %s ORDER BY 1", columnName.getColumnName(),
                    tableName, strFullWhereClause);
            LOGGER.trace(sql);
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                int id = rs.getInt(columnName.getColumnName());
                LOGGER.info(String.format("in getListOfIdsFromTable for tableName %s, columnName %s, Id=%d ", tableName,
                        columnName, id));
                list.add(id);
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static Set<Integer> getListOfIdFromSql(String selectQuery, ColumnEnum columnName) {
        Set<Integer> list = new HashSet();
        try (Connection conn = HikariCPDataSource.getConnection(); Statement stmt = conn.createStatement();) {
            LOGGER.info(selectQuery);
            ResultSet rs = stmt.executeQuery(selectQuery);
            while (rs.next()) {
                int id = rs.getInt(columnName.getColumnName());
                LOGGER.info(String.format("in getListOfIdFromSql for selectQuery[%s], columnName %s, Id=%d ",
                        selectQuery, columnName, id));
                list.add(id);
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static Set<Integer> keepNSmallestIdsFromSet(Set<Integer> setToClean, int n) {
        if (setToClean.size() < n) {
            return setToClean;
        }

        List<Integer> l1 = new ArrayList<>(setToClean);
        LOGGER.info("l1 :" + l1.toString());

        Collections.sort(l1);
        LOGGER.info("l1 sorted:" + l1.toString());

        List<Integer> l2 = l1.stream().limit(n).collect(Collectors.toList());
        LOGGER.info("l2:" + l2.toString());

        Set<Integer> res = new HashSet<Integer>(l2);
        LOGGER.info("res :" + res.toString());

        return res;
    }
}
