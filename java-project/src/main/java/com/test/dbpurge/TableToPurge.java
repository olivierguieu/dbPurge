package com.test.dbpurge;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode 

public class TableToPurge {
    public final String tableName;
    public final ColumnEnum columnEnum;

    public TableToPurge(String tableName, ColumnEnum columnEnum) {
        this.tableName = tableName;
        this.columnEnum = columnEnum;
    }

    public String toString() {
        return tableName + ":" + columnEnum.name();
    }
}
