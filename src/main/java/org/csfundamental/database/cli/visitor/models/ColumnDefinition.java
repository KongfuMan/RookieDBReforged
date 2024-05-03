package org.csfundamental.database.cli.visitor.models;

import org.csfundamental.database.table.databox.DataBox;

public final class ColumnDefinition {
    public final String name;
    public final String type;
    public final String strLen;

    public ColumnDefinition(String name, String type, String strLen) {
        this.name = name;
        this.type = type;
        this.strLen = strLen;
    }
}
