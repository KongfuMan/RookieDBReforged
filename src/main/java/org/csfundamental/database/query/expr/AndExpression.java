package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.databox.DataBox;

import java.util.ArrayList;
import java.util.List;

public class AndExpression extends Expression {
    List<NotExpression> children = new ArrayList<>();

    public void addNotExpression(NotExpression expression){
        assert expression != null;
        this.children.add(expression);
    }

    @Override
    public DataBox evaluate(Record record) {
        boolean res = true;
        for (NotExpression expr : children) {
            DataBox databox = expr.evaluate(record);
            res = res && databox.getBool();
            if (!res){
                break;
            }
        }
        return DataBox.fromObject(res);
    }
}
