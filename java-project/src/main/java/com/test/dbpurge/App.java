package com.test.dbpurge;

public class App 
{


    public static void main( String[] args )
    {
        TablePurgerProperties tablePurgerProperties = TablePurgerProperties.getInstance("src/main/resources/config.properties");
       
        TablePurger tablesPurger = new TablePurger();
        tablesPurger.run("/home/vscode/dbPurge/dbPurge/java-project/src/main/resources/tables_to_purge.yaml");
    }
}
