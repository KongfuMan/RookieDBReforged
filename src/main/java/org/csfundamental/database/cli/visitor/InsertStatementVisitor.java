package org.csfundamental.database.cli.visitor;

import org.csfundamental.database.Transaction;
import org.csfundamental.database.cli.PrettyPrinter;
import org.csfundamental.database.cli.parser.ASTIdentifier;
import org.csfundamental.database.cli.parser.ASTInsertValues;
import org.csfundamental.database.cli.parser.ASTLiteral;
import org.csfundamental.database.table.Record;
import org.csfundamental.database.table.databox.DataBox;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InsertStatementVisitor extends StatementVisitor {
    private String tableName;
    private List<Record> records;
    private List<DataBox> dataBoxes;

    @Override
    public Iterator<Record> execute(Transaction t) {
        for (Record record: this.records) {
            t.insert(tableName, record);
        }
        return null;
    }

    @Override
    public void visit(ASTIdentifier node, Object data) {
        this.tableName = node.jjtGetValue().toString();
    }

    @Override
    public void visit(ASTInsertValues node, Object data) {
        this.dataBoxes = new ArrayList<>();
        super.visit(node, data);
        this.records.add(new Record());
    }

    @Override
    public void visit(ASTLiteral node, Object data) {
        String value = node.jjtGetValue().toString();
        this.dataBoxes.add(PrettyPrinter.parseLiteral(value));
        this.records.add(new Record(this.dataBoxes));
    }
}
