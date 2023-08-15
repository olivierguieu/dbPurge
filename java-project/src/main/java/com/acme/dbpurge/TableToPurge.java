package com.acme.dbpurge;

import java.util.Set;

import lombok.Data;
import lombok.EqualsAndHashCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EqualsAndHashCode
@Data
public class TableToPurge {

    private static Logger LOGGER = LoggerFactory.getLogger("FirstLogger");

    public final String tableName;
    public final ColumnEnum columnEnum;
    public final String selectQuery;
    public final String deleteQuery;
    public final PurgerLogger purgerLogger;

    public TableToPurge(String tableName, ColumnEnum columnEnum, String selectQuery, String deleteQuery, PurgerLogger purgerLogger) {
        this.tableName = tableName;
        this.columnEnum = columnEnum;
        this.selectQuery = selectQuery;
        this.deleteQuery = deleteQuery;
        this.purgerLogger = purgerLogger;
    }

    public String toString() {
        return tableName + ":" + columnEnum.name();
    }

    public void deleteRowsFromTable(Set<Integer> listIdToPurge, Integer max_rows_to_purge_by_delete) {
        if (columnEnum == ColumnEnum.CUSTOM) {
            Utils.deleteRowsUsingDeleteQuery(deleteQuery,listIdToPurge, purgerLogger);
        } else {
            Utils.deleteRowsFromTable(tableName, columnEnum, listIdToPurge, purgerLogger, max_rows_to_purge_by_delete);
        }
    }

    public Set<Integer> getListOfIdInTable() {
        Set<Integer> list;
        if (columnEnum == ColumnEnum.CUSTOM) {
            list = Utils.getListOfIdFromSql(selectQuery, ColumnEnum.ID_STOCK);
        } else {
            list = Utils.getListOfIdInTable(tableName, columnEnum, "");
        }
        return list;
    }

    public void purgeTable() {

        Set<Integer> setIdToPurgeInDatabase = DatabasePurger.getListOfIdToPurgeInDatabase(columnEnum);
        LOGGER.info("ids to purge in database:" + setIdToPurgeInDatabase.toString());

        Set<Integer> setIdsInTable = this.getListOfIdInTable();
        LOGGER.info("ids in table:" + setIdsInTable.toString());

        setIdsInTable.retainAll(setIdToPurgeInDatabase);
        LOGGER.info("ids to purge in table:" + setIdToPurgeInDatabase.toString());

        int max_id_to_purge_by_table = DatabasePurgerProperties.getInstance(null).getDbMaxIdToPurgeByTable();
        if (max_id_to_purge_by_table!= -1) {
            LOGGER.info(String.format("Deleting max %d Ids from each table", max_id_to_purge_by_table));
            setIdsInTable = Utils.keepNSmallestIdsFromSet(setIdsInTable,
                    max_id_to_purge_by_table);
        } else {
            LOGGER.info("No limit on deleting rows from tables");
        }

        int max_rows_to_purge_by_delete =  DatabasePurgerProperties.getInstance(null).getDbMaxRowsToPurgeByDelete();

        if (setIdsInTable.size() > 0) {
            LOGGER.info(String.format("Deleting %d Ids from table '%s'...", setIdsInTable.size(), tableName));
            deleteRowsFromTable(setIdsInTable,max_rows_to_purge_by_delete);
            LOGGER.info(String.format("Deleted %d Ids from table '%s'.", setIdsInTable.size(), tableName));
        }
    }
}
