package com.test.dbpurge;

import java.util.Arrays;
import java.util.Optional;

public enum ColumnEnum {

    ID_STOCK("id_stock"), 
    ID_PROCESS("id_process");
    
    private String columName;
 
    ColumnEnum(String NewColumnName) {
        this.columName = NewColumnName;
    }
 
    public String getColumnName() {
        return columName;
    }

    public static Optional<ColumnEnum> get(String columnToIdentify) {
        return Arrays.stream(ColumnEnum.values())
            .filter(env -> env.columName.equals(columnToIdentify))
            .findFirst();
    }
}
