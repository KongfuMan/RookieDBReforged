package org.csfundamental.database.cli.visitor;

import org.csfundamental.database.Transaction;
import org.csfundamental.database.cli.parser.*;
import org.csfundamental.database.query.QueryPlan;
import org.csfundamental.database.table.Record;

import java.util.Iterator;
import java.util.Optional;

public abstract class StatementVisitor extends RookieParserDefaultVisitor {
    private StatementType type;

    public Optional<QueryPlan> getQueryPlan(Transaction t) {
        return Optional.empty();
    }

    public abstract Iterator<Record> execute(Transaction t);

    public StatementType getType() {
        return this.type;
    }
}
