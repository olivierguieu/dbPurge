package com.test.dbpurge;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {

    public void executeLiquidBaseScripts() throws SQLException, LiquibaseException {
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

    public void listAllTables() throws SQLException {
        java.sql.Connection conn = HikariCPDataSource.getConnection();
        String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='PUBLIC'";
        System.out.println(sql);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString("table_name"));
        }
        rs.close();
    }

    public void setup() {
        System.out.println("in setup &&&&&&&&&&&");

        TablePurgerProperties tablePurgerProperties = TablePurgerProperties
                .getInstance("src/test/resources/config.properties");

        try {
            executeLiquidBaseScripts();
            listAllTables();
        } catch (SQLException | LiquibaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest(String testName) {
        super(testName);

        setup();
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AppTest.class);
    }


    public void testDeletionInStockTable() {

        String tableName="STOCKA";

        TablePurger tablePurger = new TablePurger();
        List<Integer> listIdStock = tablePurger.getListOfIdsFromTable(tableName, ColumnEnum.ID_STOCK);
        System.out.println(listIdStock);

        List<Integer> listIdToCheck = Arrays.asList(1, 2, 3, 4, 5, 6);
        assertTrue(CollectionUtils.isEqualCollection(listIdStock, listIdToCheck));

        List<Integer> listIdToDelete = Arrays.asList(1, 2);
        List<Integer> listIdToCheckPostDelete = Arrays.asList(3, 4, 5, 6);
        tablePurger.deleteRowsFromTable(tableName, ColumnEnum.ID_STOCK, listIdToDelete);
        List<Integer> listIdPostDelete = tablePurger.getListOfIdsFromTable(tableName, ColumnEnum.ID_STOCK);
        assertTrue(CollectionUtils.isEqualCollection(listIdToCheckPostDelete, listIdPostDelete));
    }

    public void testDeletionInProcessTable() {

        String tableName="PROCESSA";

        TablePurger tablePurger = new TablePurger();
        List<Integer> listIdProcess = tablePurger.getListOfIdsFromTable(tableName, ColumnEnum.ID_PROCESS);
        System.out.println(listIdProcess);

        List<Integer> listIdProcessToCheck = Arrays.asList(1, 2, 3, 4, 5, 6,7);
        assertTrue(CollectionUtils.isEqualCollection(listIdProcess, listIdProcessToCheck));

        List<Integer> listIdToDelete = Arrays.asList(1, 2,5);
        List<Integer> listIdToCheckPostDelete = Arrays.asList(3, 4, 6,7);
        tablePurger.deleteRowsFromTable(tableName, ColumnEnum.ID_PROCESS, listIdToDelete);
        List<Integer> listIdPostDelete = tablePurger.getListOfIdsFromTable(tableName, ColumnEnum.ID_PROCESS);
        assertTrue(CollectionUtils.isEqualCollection(listIdToCheckPostDelete, listIdPostDelete));
    }

    public void testReadTablesToPurgeFromYamlFile() throws IOException {

        List<TableToPurge> tablesToPurge = TablePurger.readTablesToPurgeFromYamlFile("src/test/resources/tables_to_purge.yaml");

        List<TableToPurge> listTableToPurge = new ArrayList<>();
        listTableToPurge.add(new TableToPurge("STOCKA", ColumnEnum.ID_STOCK));
        listTableToPurge.add(new TableToPurge("STOCKB", ColumnEnum.ID_STOCK));
        listTableToPurge.add(new TableToPurge("PROCESSA", ColumnEnum.ID_PROCESS));

/* 
        for (TableToPurge  tableToPurge : tablesToPurge) {
            System.out.println(tableToPurge.toString());
        }
        
        for (TableToPurge  tableToPurge : listTableToPurge) {
            System.out.println(tableToPurge.toString());
        }
*/
        assertTrue(CollectionUtils.isEqualCollection(tablesToPurge, listTableToPurge));
    }
    


}
