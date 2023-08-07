package com.acme.dbpurge;

public enum ColumnEnum {

    ID_STOCK("id_stock"), 
    ID_PROCESS("id_process"),
    CUSTOM("custom");
    
    
    private String columName;
 
    ColumnEnum(String NewColumnName) {
        this.columName = NewColumnName;
    }
 
    public String getColumnName() {
        return columName;
    }
}
