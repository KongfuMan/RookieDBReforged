package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.databox.DataBox;

import java.util.ArrayList;
import java.util.List;

public class OrExpression extends Expression {
    List<AndExpression> children = new ArrayList<>();
    public void addAndExpression(AndExpression expr){
        this.children.add(expr);
    }

    @Override
    public DataBox evaluate(Record record) {
        boolean res = false;
        for (AndExpression expr : children) {
            DataBox databox = expr.evaluate(record);
            res = res || databox.getBool();
            if (res){
                break;
            }
        }
        return DataBox.fromObject(res);
    }
}
