package org.csfundamental.database.cli.visitor;

import org.csfundamental.database.Transaction;
import org.csfundamental.database.cli.parser.ASTColumnDef;
import org.csfundamental.database.cli.parser.ASTIdentifier;
import org.csfundamental.database.cli.parser.ASTSelectStatement;
import org.csfundamental.database.cli.visitor.models.ColumnDefinition;
import org.csfundamental.database.query.QueryPlan;
import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.Schema;
import org.csfundamental.database.table.databox.Type;

import java.util.Iterator;
import java.util.List;

public class CreateTableStatementVisitor extends StatementVisitor {
    private String tableName;
    private List<ColumnDefinition> columnDef;
    private Schema schema = new Schema();
    private ASTSelectStatement selectStmt;
    private SelectStatementVisitor selectStmtVisitor;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTColumnDef node, Object data) {
        Object[] res = (Object[])node.jjtGetValue();
        schema.add((String)res[0], Type.fromStringLiteral((String)res[1], (String)res[2]));
    }

    @Override
    public void visit(ASTSelectStatement node, Object data) {
        // defer the select operation to the execution phase.
        this.selectStmt = node;
    }

    @Override
    public Iterator<Record> execute(Transaction t) {
        if (this.selectStmt != null){
            this.selectStmtVisitor = new SelectStatementVisitor();
            this.selectStmt.jjtAccept(this.selectStmtVisitor, null);

            QueryPlan queryPlan = this.selectStmtVisitor.getQueryPlan(t).get();
            Iterator<Record> records = queryPlan.execute();
            this.schema = queryPlan.getFinalOperator().getSchema();
        }

        t.createTable(this.schema, this.tableName);
        return null;
    }
}
