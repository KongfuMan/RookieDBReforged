package org.csfundamental.database.cli.visitor;

import org.csfundamental.database.cli.PrettyPrinter;
import org.csfundamental.database.cli.parser.ASTColumnName;
import org.csfundamental.database.cli.parser.ASTComparisonOperator;
import org.csfundamental.database.cli.parser.ASTLiteral;
import org.csfundamental.database.cli.parser.RookieParserDefaultVisitor;
import org.csfundamental.database.cli.visitor.models.Predicate;
import org.csfundamental.database.query.expr.ComparisonExpressionOperator;
import org.csfundamental.database.table.databox.DataBox;

// WHERE column_name comp_op literal
public class ColumnValueComparisonVisitor extends RookieParserDefaultVisitor {
    String column;
    ComparisonExpressionOperator op;

    DataBox dataBox;

    @Override
    public void visit(ASTColumnName node, Object data) {
        this.column = node.jjtGetValue().toString();
    }

    @Override
    public void visit(ASTComparisonOperator node, Object data) {
        this.op = new ComparisonExpressionOperator(node.jjtGetValue().toString());
    }

    @Override
    public void visit(ASTLiteral node, Object data) {
        var numStr = node.jjtGetValue().toString();
        this.dataBox = PrettyPrinter.parseLiteral(numStr);
    }

    public Predicate build(){
        return new Predicate(this.column, this.op, this.dataBox);
    }
}
