package org.csfundamental.database.cli.visitor.models;

import org.csfundamental.database.query.expr.Expression;

public class SelectItem {
    private String column;
    private String alias;
    private Expression expr;

    private void init(String column, String alias, Expression expr){
        this.column = column;
        this.alias = alias;
        this.expr = expr;
    }

    public SelectItem(String column, String alias, Expression expr) {
        assert column != null && alias != null && expr != null;
        this.init(column, alias, expr);
    }

    public SelectItem(String column, String alias) {
        assert column != null && alias != null;
        this.init(column, alias, null);
    }

    public SelectItem(String column) {
        assert column != null;
        this.init(column, null, null);
    }

    public SelectItem() {}

    public SelectItem setColumn(String column){
        assert column != null;
        this.column = column;
        return this;
    }

    public SelectItem setAlias(String alias){
        assert alias != null;
        this.alias = alias;
        return this;
    }

    public SelectItem setExpression(Expression expr){
        assert expr != null;
        this.expr = expr;
        return this;
    }
}
