package org.csfundamental.database.cli.visitor;

import org.csfundamental.database.Database;
import org.csfundamental.database.cli.parser.*;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class StatementListVisitor extends RookieParserDefaultVisitor {
    List<StatementVisitor> statementVisitors = new ArrayList<>();

    public StatementListVisitor(Database db, PrintStream out) {
    }

    @Override
    public void visit(ASTExecutableStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTExplainStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTDropTableStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTDropIndexStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTReleaseStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTSavepointStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTRollbackStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTBeginStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTCommitStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTInsertStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTUpdateStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTSelectStatement node, Object data) {
        var visitor = new SelectStatementVisitor();
        node.jjtAccept(visitor, data);
        this.statementVisitors.add(visitor);
    }

    @Override
    public void visit(ASTDeleteStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTCreateTableStatement node, Object data) {
        super.visit(node, data);
    }

    @Override
    public void visit(ASTCreateIndexStatement node, Object data) {
        super.visit(node, data);
    }
}
