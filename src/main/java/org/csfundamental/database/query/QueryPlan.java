package org.csfundamental.database.query;

import org.csfundamental.database.Transaction;
import org.csfundamental.database.cli.visitor.models.AliasedTable;
import org.csfundamental.database.cli.visitor.models.Predicate;
import org.csfundamental.database.table.Record;

import java.util.Iterator;
import java.util.List;

public class QueryPlan {
    private QueryOperator finalOperator;
    private List<AliasedTable> tables;

    public Iterator<Record> execute() {
        // generate a optimal DAG of QueryOperators
        return null;
    }

    public QueryOperator getFinalOperator() {
        return this.finalOperator;
    }

    public void addTempTableAlias(AliasedTable aliasedTable) {

    }

    public void join(AliasedTable aliasedTable, String leftCol, String rightCol) {

    }

    public void select(Predicate predicate) {

    }

    public void groupBy(List<String> groupBy) {
    }

    public void orderBy(String orderByColumn) {
    }

    public void limit(int limit) {
    }
}
