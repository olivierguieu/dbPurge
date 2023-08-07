package com.acme.dbpurge;

import java.util.Set;
import com.google.common.collect.Sets;

public class DatabasePurger {

    // initialized only once sets of id (stock/process) that can be purged in the database
    private static Set<Integer> setIdProcessToPurgeInDatabase = null;
    private static Set<Integer> setIdStockToPurge = null;

    public static Set<Integer>  getListOfIdToPurgeInDatabase(ColumnEnum columnEnum) {
        if (columnEnum == ColumnEnum.ID_PROCESS) {
            return DatabasePurger.getListOfIdProcessToPurgeInDatabase();
         } else {
            return DatabasePurger.getListOfIdStockToPurgeInDatabase();
         }
    }

    public static Set<Integer> getListOfIdProcessToPurgeInDatabase() {
        if (setIdProcessToPurgeInDatabase == null) {
            setIdProcessToPurgeInDatabase = Utils.getListOfIdInTable("ta_prm_process", ColumnEnum.ID_PROCESS,
                    "WHERE lower(flg_keep_process)='false'");
        }
        return setIdProcessToPurgeInDatabase;
    }

    public static Set<Integer> getListOfIdStockToPurgeInDatabase() {
        if (setIdStockToPurge == null) {
            Set<Integer> listIdStockWithKeepProcessFalse = Utils.getListOfIdInTable("ta_prm_stock",
                    ColumnEnum.ID_STOCK, "");
            Set<Integer> listIdStockWithKeepProcessTrue = Utils.getListOfIdInTable("ta_prm_process",
                    ColumnEnum.ID_STOCK, "WHERE lower(flg_keep_process)='true'");

            setIdStockToPurge = Sets.difference(listIdStockWithKeepProcessFalse, listIdStockWithKeepProcessTrue);
        }
        return setIdStockToPurge;
    }
}
