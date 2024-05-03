package org.csfundamental.database.cli.visitor;

import org.csfundamental.database.cli.PrettyPrinter;
import org.csfundamental.database.cli.parser.*;
import org.csfundamental.database.query.expr.*;
import org.csfundamental.database.table.databox.DataBox;


/**
 * Visit ASTExpression and all its descendant nodes and generate Expression instance
 * expr:
 *  or_expr
 *    and_expr ( or and_expr)*
 *      not_expr (and not_expr)*
 *          (not_op)* comp_expr
 *              add_expr (comp_op add_expr)*
 *                  multi_expr (add_op multi_expr)*
 *                      prim_expr (multi_op prim_expr)*
 *                          str_literal | func_expr | column_name | expr | add_op prim_expr
* */
public class ExpressionVisitor extends RookieParserDefaultVisitor {
    Expression expr;

    @Override
    public void visit(ASTOrExpression node, Object data) {
        OrExpressionVisitor visitor = new OrExpressionVisitor();
        node.jjtAccept(visitor, data);
        expr = visitor.expr;
    }

    class OrExpressionVisitor extends RookieParserDefaultVisitor {
        OrExpression expr = new OrExpression();

        @Override
        public void visit(ASTAndExpression node, Object data) {
            AndExpressionVisitor visitor = new AndExpressionVisitor();
            node.jjtAccept(visitor, data);
            expr.addAndExpression(visitor.expr);
        }
    }

    class AndExpressionVisitor extends RookieParserDefaultVisitor {
       AndExpression expr = new AndExpression();

        @Override
        public void visit(ASTNotExpression node, Object data) {
            NotExpressionVisitor visitor = new NotExpressionVisitor();
            node.jjtAccept(visitor, data);
            expr.addNotExpression(visitor.expr);
        }
    }

    class NotExpressionVisitor extends RookieParserDefaultVisitor {
        NotExpression expr = new NotExpression();

        @Override
        public void visit(ASTNotOperator node, Object data) {
            expr.addHasNot();
        }

        @Override
        public void visit(ASTComparisonExpression node, Object data) {
            ComparisonExpressionVisitor visitor = new ComparisonExpressionVisitor();
            node.jjtAccept(visitor, data);
            expr.setBinaryExpr(visitor.expr);
        }
    }

    class ComparisonExpressionVisitor extends RookieParserDefaultVisitor {
        BinaryExpression expr;
        private ExpressionOperator Op;

        @Override
        public void visit(ASTAdditiveExpression node, Object data) {
            AdditiveExpressionVisitor visitor = new AdditiveExpressionVisitor();
            node.jjtAccept(visitor, data);
            this.expr = new ComparisonExpression(this.Op, visitor.expr, this.expr);
        }

        @Override
        public void visit(ASTComparisonOperator node, Object data) {
            String op = (String)node.jjtGetValue();
            this.Op = new ComparisonExpressionOperator(op);
        }
    }

    class AdditiveExpressionVisitor extends RookieParserDefaultVisitor {
        BinaryExpression expr;
        private ExpressionOperator Op;

        @Override
        public void visit(ASTMultiplicativeExpression node, Object data) {
            MultiplicativeExpressionVisitor visitor = new MultiplicativeExpressionVisitor();
            node.jjtAccept(visitor, data);
            this.expr = new AdditiveExpression(this.Op, visitor.expr, this.expr);
        }

        @Override
        public void visit(ASTAdditiveOperator node, Object data) {
            String op = (String)node.jjtGetValue();
            this.Op = new AdditiveExpressionOperator(op);
        }
    }

    /**
     * pri_expr multi_op pri_expr.
     * Left associative property.
     * */
    class MultiplicativeExpressionVisitor extends RookieParserDefaultVisitor {
        BinaryExpression expr; // tail of a linked list of bin expr
        private ExpressionOperator Op;

        @Override
        public void visit(ASTPrimaryExpression node, Object data) {
            PrimaryExpressionVisitor visitor = new PrimaryExpressionVisitor();
            node.jjtAccept(visitor,data);
            this.expr = new MultiplicativeExpression(this.Op, visitor.expr, this.expr);
        }

        @Override
        public void visit(ASTMultiplicativeOperator node, Object data) {
            String op = (String)node.jjtGetValue();
            this.Op = new MultiplicativeExpressionOperator(op);
        }
    }

    class PrimaryExpressionVisitor extends RookieParserDefaultVisitor {
        PrimaryExpression expr = new PrimaryExpression();

        private boolean seenRoot = false;

        @Override
        public void visit(ASTLiteral node, Object data) {
            DataBox databox = PrettyPrinter.parseLiteral((String) node.jjtGetValue());
            this.expr.setChildExpr(new Literal(databox));
        }

        @Override
        public void visit(ASTFunctionCallExpression node, Object data) {
            FunctionCallExpressionVisitor visitor = new FunctionCallExpressionVisitor();
            node.jjtAccept(visitor, data);
            expr.setChildExpr(visitor.funcCallExpr);
        }

        @Override
        public void visit(ASTColumnName node, Object data) {
            String column = (String)node.jjtGetValue();
            this.expr.setChildExpr(new Column(column));
        }

        @Override
        public void visit(ASTExpression node, Object data) {
            ExpressionVisitor visitor = new ExpressionVisitor();
            node.jjtAccept(visitor, data);
            this.expr.setChildExpr(visitor.expr);
        }

        @Override
        public void visit(ASTAdditiveOperator node, Object data) {
            AdditiveExpressionVisitor visitor = new AdditiveExpressionVisitor();
            node.jjtAccept(visitor, data);
            this.expr.setAdditiveOperator(new AdditiveExpressionOperator((String)node.jjtGetValue()));
        }

        @Override
        public void visit(ASTPrimaryExpression node, Object data) {
            if (!seenRoot) {
                // root pri, simply visit its children.
                super.visit(node, data);
            } else {
                PrimaryExpressionVisitor visitor = new PrimaryExpressionVisitor();
                node.jjtAccept(visitor, data);
                this.expr.setChildExpr(visitor.expr);
                seenRoot = true;
            }
        }
    }

    //TODO: FunctionCallExpression should be an abstract class, implemented by
    // various functions, e.g. count, average, max, etc.
    class FunctionCallExpressionVisitor extends RookieParserDefaultVisitor {
        FunctionCallExpression funcCallExpr = new FunctionCallExpression();

        @Override
        public void visit(ASTIdentifier node, Object data) {
            funcCallExpr.setFuncName((String)node.jjtGetValue());
        }

        @Override
        public void visit(ASTExpression node, Object data) {
            ExpressionVisitor visitor = new ExpressionVisitor();
            node.jjtAccept(visitor, data);
            funcCallExpr.addArgument(visitor.expr);
        }
    }
}
