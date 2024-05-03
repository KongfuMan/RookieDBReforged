package org.csfundamental.database.query.expr;

import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.databox.DataBox;

import java.util.ArrayList;
import java.util.List;

public class FunctionCallExpression extends Expression {
    String funcName;
    List<Expression> arguments;

    public FunctionCallExpression() {
        this.arguments = new ArrayList<>();
    }

    public void setFuncName(String funcName){
        assert funcName != null;
        this.funcName = funcName;
    }

    public void addArgument(Expression argument){
        assert argument != null;
        this.arguments.add(argument);
    }

    @Override
    public DataBox evaluate(Record record) {
        return null;
    }

//    @Override
//    public Object evaluate(Record record) {
//        return null;
//    }
}
