package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.databox.DataBox;
import org.csfundamental.database.table.databox.TypeId;

public class NotExpression extends Expression{
    boolean hasNot = false;
    BinaryExpression comparisonExpr;

    public void setBinaryExpr(BinaryExpression expr){
        assert expr != null;
        this.comparisonExpr = expr;
    }

    public void addHasNot(){
        this.hasNot ^= true;
    }

    @Override
    public DataBox evaluate(Record record) {
        DataBox dataBox = this.comparisonExpr.evaluate(record);
        if (dataBox.getTypeId() != TypeId.BOOL){
            throw new RuntimeException("");
        }
        if (!hasNot){
            return DataBox.fromObject(!dataBox.getBool());
        }
        throw new RuntimeException("unknown type.");
    }
}
