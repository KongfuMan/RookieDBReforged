package org.csfundamental.database.cli.visitor.models;

public class AliasedTable {
    String table;
    String alias;

    public AliasedTable(String[] names){
        this.table = names[0];
        this.alias = names[1] != null ? names[1] : names[0];
    }

    public AliasedTable(String tableName){
        this.table = tableName;
        this.alias = tableName;
    }
}
