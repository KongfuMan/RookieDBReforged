package org.csfundamental.database.cli.visitor;

import org.csfundamental.database.Transaction;
import org.csfundamental.database.TransactionContext;
import org.csfundamental.database.cli.parser.ASTColumnName;
import org.csfundamental.database.cli.parser.ASTIdentifier;
import org.csfundamental.database.cli.parser.ASTSelectStatement;
import org.csfundamental.database.cli.parser.RookieParserDefaultVisitor;
import org.csfundamental.database.cli.visitor.models.AliasedTable;
import org.csfundamental.database.common.Pair;
import org.csfundamental.database.query.QueryPlan;
import org.csfundamental.database.query.expr.Column;
import org.csfundamental.database.table.Schema;

import java.util.List;

// with mycte(col1, col2) as ( select_stmt)
public class CommonTableExpressionVisitor extends RookieParserDefaultVisitor {
    String name; // cte name
    String alias;
    List<Column> columnList;
    ASTSelectStatement selectStmt;
    SelectStatementVisitor selectStmtVisitor;

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.name = node.jjtGetValue().toString();
    }

    @Override
    public void visit(ASTColumnName node, Object data) {
        String column =  node.jjtGetValue().toString();
        this.columnList.add(new Column(column));
    }

    @Override
    public void visit(ASTSelectStatement node, Object data) {
        this.selectStmt = node;
    }

    public void createTable(Transaction t, List<AliasedTable> contextTables){
        // create table schema out of selectStmtVisitor of CTE
        this.selectStmtVisitor = new SelectStatementVisitor();
        this.selectStmtVisitor.setContextTables(contextTables);
        this.selectStmt.jjtAccept(this.selectStmtVisitor, null);

        QueryPlan qp = this.selectStmtVisitor.getQueryPlan(t).get();
        qp.execute();
        Schema schema = qp.getFinalOperator().getSchema();
        if (this.columnList.size() != 0){
            if (this.columnList.size() != schema.size()){
                throw new RuntimeException("");
            }

            Schema prev = schema;
            schema = new Schema();
            for (int i = 0; i < prev.size(); i++){
                schema.add(this.columnList.get(i).toString(), prev.getFieldType(i));
            }
        }
        TransactionContext tc = t.getTransactionContext();
        this.alias = tc.createTempTable(schema);
    }

    public void populate(Transaction t){

    }

    public AliasedTable getAliasedTable(){
        return new AliasedTable(new String[]{this.name, this.alias});
    }
}
