package org.csfundamental.database.cli.visitor;

import org.csfundamental.database.Transaction;
import org.csfundamental.database.cli.parser.*;
import org.csfundamental.database.cli.visitor.models.AliasedTable;
import org.csfundamental.database.cli.visitor.models.Predicate;
import org.csfundamental.database.cli.visitor.models.SelectItem;
import org.csfundamental.database.query.QueryPlan;
import org.csfundamental.database.table.Record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class SelectStatementVisitor extends StatementVisitor{
    private List<CommonTableExpressionVisitor> cteVisitors = new ArrayList<>();
    private List<SelectItem> selectItems = new ArrayList<>();
    private List<AliasedTable> tables = new ArrayList<>();
    private List<String> leftJoinedColumns = new ArrayList<>();
    private List<String> rightJoinedColumns = new ArrayList<>();
    private List<Predicate> predicates = new ArrayList<>();
    private List<String> groupBy = new ArrayList<>();
    private String orderByColumn;
    private int limit;

    private List<AliasedTable> contextTables = new ArrayList<>();

    public void setContextTables(List<AliasedTable> contextTables){
        this.contextTables = contextTables;
    }

    @Override
    public Optional<QueryPlan> getQueryPlan(Transaction t){
        List<AliasedTable> tempContext = new ArrayList<>(this.contextTables); // deep copy.
        for (var visitor : this.cteVisitors) {
            visitor.createTable(t, tempContext);
            tempContext.add(visitor.getAliasedTable());
        }

        QueryPlan queryPlan = new QueryPlan();
        for (var aliasedTable : tempContext){
            queryPlan.addTempTableAlias(aliasedTable);
        }

        for (int i = 0; i < this.tables.size(); i++){
            queryPlan.join(
                    this.tables.get(i),
                    this.leftJoinedColumns.get(i),
                    this.rightJoinedColumns.get(i)
            );
        }

        for (int i = 0; i < this.predicates.size(); i++){
            queryPlan.select(this.predicates.get(i));
        }

        for (int i = 0; i < this.selectItems.size(); i++){
            SelectItem name = selectItems.get(i);
            // case1. selected column is expression
            // case2. selected column == "*"
            // case3. selected column is "table.*"
        }

        queryPlan.groupBy(this.groupBy);
        queryPlan.orderBy(this.orderByColumn);
        queryPlan.limit(this.limit);

        // CTE populate
        for (int i = 0; i < this.cteVisitors.size(); i++){
            this.cteVisitors.get(i).populate(t);
        }

        return Optional.of(queryPlan);
    }

    @Override
    public Iterator<Record> execute(Transaction t){
        QueryPlan queryPlan = this.getQueryPlan(t).get();
        return queryPlan.execute();
    }

    @Override
    public void visit(ASTCommonTableExpression node, Object data) {
        var visitor = new CommonTableExpressionVisitor();
        node.jjtAccept(visitor, data);
        cteVisitors.add(visitor);
    }

    // SELECT *,  table.*,  expr (AS alias)?
    // expr could be column string or expression
    @Override
    public void visit(ASTSelectColumn node, Object data) {
        var item = new SelectItem();
        if (node.jjtGetValue().toString().startsWith("<>")) {
            // expr  (AS | alias)?
            super.defaultVisit(node, item);
        }

        var column = (String)node.jjtGetValue();
        if (column.toUpperCase().contains(" AS ")) {
            // add alias
            int o = column.toUpperCase().indexOf(" AS ");
            item = item.setAlias(column.substring(o + 4).trim());
            column = column.substring(0, o);
        }

        if (node.jjtGetValue().toString().startsWith("<>")) {
            return;
        }

        item.setColumn(column);
        selectItems.add(item);
    }

    @Override
    public void visit(ASTExpression node, Object data) {
        ExpressionVisitor visitor = new ExpressionVisitor();
        node.jjtAccept(visitor, data);
        if (!(data instanceof SelectItem item)){
            throw new RuntimeException("");
        }
        item.setExpression(visitor.expr);
        selectItems.add(item);
    }

    // FROM aliased_table_name()
    // "FROM table1 AS alias1"
    @Override
    public void visit(ASTAliasedTableName node, Object data) {
        String[] aliasedTable = (String[])node.jjtGetValue();
        this.tables.add(new AliasedTable(aliasedTable));
    }

    // JOIN aliased_table_name() ON column_name (=column_name())?
    // "JOIN table2 AS alias2 ON column = column"
    @Override
    public void visit(ASTJoinedTable node, Object data) {
        String[] columns = (String[])node.jjtGetValue();
        this.leftJoinedColumns.add(columns[0]);
        this.rightJoinedColumns.add(columns[1]);
        // explicitly visit the first child "aliased_table_name", otherwise it won't be visited.
        node.jjtGetChild(0).jjtAccept(this, data);
    }

    // WHERE column_name comp_op literal
    @Override
    public void visit(ASTColumnValueComparison node, Object data) {
        ColumnValueComparisonVisitor visitor = new ColumnValueComparisonVisitor();
        node.jjtAccept(visitor, data);
        this.predicates.add(visitor.build());
    }

    // GROUP BY column_name
    @Override
    public void visit(ASTColumnName node, Object data) {
        this.groupBy.add(node.jjtGetValue().toString());
    }

    @Override
    public void visit(ASTOrderClause node, Object data) {
        this.orderByColumn = node.jjtGetValue().toString();
    }

    @Override
    public void visit(ASTLimitClause node, Object data) {
        this.limit = Integer.parseInt(node.jjtGetValue().toString());
    }
}
