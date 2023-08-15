
package com.acme.dbpurge;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.IOException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;

import org.junit.jupiter.api.Test;

import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Sets;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppTest {

    private static Logger LOGGER = LoggerFactory.getLogger("FirstLogger");
    private PurgerLogger purgerLogger = null;
    private DatabasePurgerProperties databasePurgerProperties = null;

    public static void executeLiquidBaseScripts() throws SQLException, LiquibaseException {
        java.sql.Connection connection = HikariCPDataSource.getConnection();

        try {
            Database database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(connection));
            Liquibase liquibase = new Liquibase("db/changelog/app-changelog.xml", new ClassLoaderResourceAccessor(),
                    database);
            liquibase.update(new Contexts());
        } finally {
            if (connection != null) {
                connection.rollback();
                connection.close();
            }
        }
    }

    public static void listAllTablesInH2Database() throws SQLException {
        java.sql.Connection conn = HikariCPDataSource.getConnection();
        String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='PUBLIC'";
        LOGGER.info(sql);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            LOGGER.info(rs.getString("table_name"));
        }
        rs.close();
    }

    public void init() {
        databasePurgerProperties = DatabasePurgerProperties.getInstance("src/test/resources/config.properties");

        try {
            executeLiquidBaseScripts();
            listAllTablesInH2Database();
        } catch (SQLException | LiquibaseException e) {
            e.printStackTrace();
        }

        purgerLogger = new PurgerLogger();
        purgerLogger.init();

    }

    public void end() {
        purgerLogger.stop();
    }

    @Test
    public void test_deleteRowsFromTable_1() {

        init();

        TableToPurge tableToPurge = new TableToPurge("STOCKA", ColumnEnum.ID_STOCK, null, null, purgerLogger);

        Set<Integer> setIdStock = tableToPurge.getListOfIdInTable();
        LOGGER.info(setIdStock.toString());

        Set<Integer> listIdToCheck = Sets.newHashSet(1, 2, 3, 4, 5, 6);
        assertTrue(CollectionUtils.isEqualCollection(setIdStock, listIdToCheck));

        Set<Integer> setIdToDelete = Sets.newHashSet(1, 2);
        Set<Integer> setIdToCheckPostDelete = Sets.newHashSet(3, 4, 5, 6);
        tableToPurge.deleteRowsFromTable(setIdToDelete,2);
        Set<Integer> setIdPostDelete = tableToPurge.getListOfIdInTable();
        assertTrue(CollectionUtils.isEqualCollection(setIdToCheckPostDelete, setIdPostDelete));

        end();
    }

    @Test
    public void test_deleteRowsFromTable_2() {

        init();

        TableToPurge tableToPurge = new TableToPurge("PROCESSA", ColumnEnum.ID_PROCESS, null, null, purgerLogger);

        Set<Integer> setIdProcess = tableToPurge.getListOfIdInTable();
        LOGGER.info(setIdProcess.toString());

        Set<Integer> listIdProcessToCheck = Sets.newHashSet(1, 2, 3, 4, 5, 6, 7);
        assertTrue(CollectionUtils.isEqualCollection(setIdProcess, listIdProcessToCheck));

        Set<Integer> listIdToDelete = Sets.newHashSet(1, 2, 5);
        Set<Integer> listIdToCheckPostDelete = Sets.newHashSet(3, 4, 6, 7);
        tableToPurge.deleteRowsFromTable(listIdToDelete,2);
        Set<Integer> listIdPostDelete = tableToPurge.getListOfIdInTable();
        assertTrue(CollectionUtils.isEqualCollection(listIdToCheckPostDelete, listIdPostDelete));

        end();
    }

    @Test
    public void test_deleteRowsFromTable_3() {

        init();

        TableToPurge tableToPurge = new TableToPurge("CUSTOMA", ColumnEnum.ID_STOCK, null, null, purgerLogger);

        Set<Integer> setIdStock = tableToPurge.getListOfIdInTable();
        LOGGER.info(setIdStock.toString());

        Set<Integer> listIdToCheck = Sets.newHashSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19);
        assertTrue(CollectionUtils.isEqualCollection(setIdStock, listIdToCheck));

        Set<Integer> setIdToDelete = Sets.newHashSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 19);
        Set<Integer> setIdToCheckPostDelete = Sets.newHashSet(12, 13, 14, 15, 16, 17, 18);
        tableToPurge.deleteRowsFromTable(setIdToDelete,2);
        Set<Integer> setIdPostDelete = tableToPurge.getListOfIdInTable();

        LOGGER.info(setIdPostDelete.toString());
        LOGGER.info(setIdToCheckPostDelete.toString());

        assertTrue(CollectionUtils.isEqualCollection(setIdToCheckPostDelete, setIdPostDelete));

        end();
    }

    @Test
    public void test_readTablesToPurgeFromYamlFile_1() throws IOException {

        init();

        List<TableToPurge> tablesToPurge = Utils
                .readTablesToPurgeFromYamlFile("src/test/resources/tables_to_purge.yaml", purgerLogger);
        List<TableToPurge> listTableToPurge = new ArrayList<>();

        listTableToPurge.add(new TableToPurge("STOCKA", ColumnEnum.ID_STOCK, null, null, purgerLogger));
        listTableToPurge.add(new TableToPurge("STOCKB", ColumnEnum.ID_STOCK, null, null, purgerLogger));
        listTableToPurge.add(new TableToPurge("PROCESSA", ColumnEnum.ID_PROCESS, null, null, purgerLogger));
        listTableToPurge.add(new TableToPurge("CUSTOMA", ColumnEnum.CUSTOM, "select id_stock from CUSTOMA",
                "delete CUSTOMA where id_stock=%d", purgerLogger));

        assertTrue(CollectionUtils.isEqualCollection(tablesToPurge, listTableToPurge));

        end();
    }

    @Test
    public void test_getListOfIdProcessToPurgeInDatabase_1() throws IOException {

        init();

        Set<Integer> setIdProcessToPurge = DatabasePurger.getListOfIdProcessToPurgeInDatabase();
        LOGGER.info(setIdProcessToPurge.toString());

        Set<Integer> targetListIdProcessToPurge = Sets.newHashSet(2, 4, 6, 8, 10, 16, 17, 18, 19);
        assertTrue(CollectionUtils.isEqualCollection(targetListIdProcessToPurge, setIdProcessToPurge));

        end();
    }

    @Test
    public void test_getListOfIdStockToPurgeInDatabase_1() throws IOException {

        init();

        Set<Integer> setIdStockToPurge = DatabasePurger.getListOfIdStockToPurgeInDatabase();
        LOGGER.info("id_stock to purge in database:" + setIdStockToPurge.toString());

        Set<Integer> targetListIdStockToPurge = Sets.newHashSet(6, 7, 8, 9, 10, 11, 12, 13);
        assertTrue(CollectionUtils.isEqualCollection(targetListIdStockToPurge, setIdStockToPurge));

        end();
    }

    @Test
    public void test_purgeTable_1() {

        init();

        TableToPurge tableToPurge = new TableToPurge("PROCESSC", ColumnEnum.ID_PROCESS, null, null, purgerLogger);

        Set<Integer> setIdProcess = tableToPurge.getListOfIdInTable();
        LOGGER.info("set of ids in table:" + setIdProcess.toString());

        Set<Integer> setIdProcessToCheck = Sets.newHashSet(1, 2, 3, 4, 5, 6, 7);
        assertTrue(CollectionUtils.isEqualCollection(setIdProcess, setIdProcessToCheck));

        int before = databasePurgerProperties.getDbMaxIdToPurgeByTable();
        databasePurgerProperties.setDbMaxIdToPurgeByTable(10);

        try (MockedStatic<DatabasePurger> utilities = Mockito.mockStatic(DatabasePurger.class)) {

            Set<Integer> setIdToPurge = Sets.newHashSet(1, 3, 5, 7);
            utilities.when(() -> DatabasePurger.getListOfIdToPurgeInDatabase(ColumnEnum.ID_PROCESS))
                    .thenReturn(setIdToPurge);
            tableToPurge.purgeTable();

            Set<Integer> setIdProcessPostPurge = tableToPurge.getListOfIdInTable();
            LOGGER.info(setIdProcessPostPurge.toString());

            Set<Integer> targetSetIdProcessPostPurge = Sets.newHashSet(2, 4, 6);
            LOGGER.info("expected ids in table (post purge):" + targetSetIdProcessPostPurge.toString());
            assertTrue(CollectionUtils.isEqualCollection(setIdProcessPostPurge, targetSetIdProcessPostPurge));
        }

        databasePurgerProperties.setDbMaxIdToPurgeByTable(before);

        end();
    }

    @Test
    public void test_purgeTable_2() {

        init();

        TableToPurge tableToPurge = new TableToPurge("PROCESSD", ColumnEnum.ID_PROCESS, null, null, purgerLogger);

        Set<Integer> setIdProcess = tableToPurge.getListOfIdInTable();
        LOGGER.info("set of ids in table:" + setIdProcess.toString());

        Set<Integer> setIdProcessToCheck = Sets.newHashSet(1, 2, 3, 4, 5, 6, 7);
        assertTrue(CollectionUtils.isEqualCollection(setIdProcess, setIdProcessToCheck));

        int before = databasePurgerProperties.getDbMaxIdToPurgeByTable();
        databasePurgerProperties.setDbMaxIdToPurgeByTable(2);

        try (MockedStatic<DatabasePurger> utilities = Mockito.mockStatic(DatabasePurger.class)) {

            Set<Integer> setIdToPurge = Sets.newHashSet(1, 3, 5, 7);
            utilities.when(() -> DatabasePurger.getListOfIdToPurgeInDatabase(ColumnEnum.ID_PROCESS))
                    .thenReturn(setIdToPurge);
            tableToPurge.purgeTable();

            Set<Integer> setIdProcessPostPurge = tableToPurge.getListOfIdInTable();
            LOGGER.info(setIdProcessPostPurge.toString());

            Set<Integer> targetSetIdProcessPostPurge = Sets.newHashSet(2,4,5,6,7);
            LOGGER.info("expected ids in table (post purge):" + targetSetIdProcessPostPurge.toString());
            assertTrue(CollectionUtils.isEqualCollection(setIdProcessPostPurge, targetSetIdProcessPostPurge));
        }

        databasePurgerProperties.setDbMaxIdToPurgeByTable(before);

        end();
    }
}
