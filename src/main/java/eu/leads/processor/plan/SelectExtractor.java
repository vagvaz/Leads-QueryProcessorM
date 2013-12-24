package eu.leads.processor.plan;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.operators.*;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.utils.SQLUtils;
import eu.leads.processor.utils.math.MathOperatorTree;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Limit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/21/13
 * Time: 1:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class SelectExtractor implements BasicPlannerExtractor {
    private JsonNode root = null;
    private final ExecutionPlan plan;
    private QueryContext context;
    private String output;
    private final ObjectMapper mapper;
    private Limit limit;
    private final List<Column> columns;
    private final List<Column> groupByColumns;
    private final List<Column> orderByColumns;
    private final List<Table> allTable;
    private List<Function> functions = new ArrayList<Function>();
    private boolean allColumns;
    private final List<Boolean> ascendingOrder;
    private Table fromTable;
    private MathOperatorTree havingTree;
    private MathOperatorTree whereTree;
    private Table joinTable;
    private Column joinColumnA;
    private Column joinColumnB;

    public SelectExtractor(JsonNode node) {
        root = node;
        plan = new ExecutionPlan();
        mapper = new ObjectMapper();
        limit = null;
        columns = new ArrayList<Column>();
        allTable = new ArrayList<Table>();
        functions = new ArrayList<Function>();
        groupByColumns = new ArrayList<Column>();
        orderByColumns = new ArrayList<Column>();
        ascendingOrder = new ArrayList<Boolean>();
        allColumns = false;
        whereTree = null;
        havingTree = null;

    }


    @Override
    public ExecutionPlan extractPlan(String outputNode, QueryContext context) {
        //add output node to plan
        this.context = context;
        output = outputNode;
        ExecutionPlanNode node = new OutputOperator(outputNode);
        plan.setOutput(node);

        //Extract information in order to generate basic plan
        // Extract from
        JsonNode fromNode = root.path("selectBody").path("fromItem");
        extractFromPart(fromNode);
        //Extract join
        JsonNode joins = root.path("selectBody").path("joins");
        if (!joins.isNull())
            extractJoinPart(joins);
        //Extract Filter
        JsonNode where = root.path("selectBody").path("where");
        if (!where.isNull())
            extractWherePart(where);
        //Extract Project
        JsonNode selectColumns = root.path("selectBody").path("selectItems");
        extractSelectItemsPart(selectColumns);
        //Extract GroupBy
        JsonNode groupBy = root.path("selectBody").path("groupByColumnReferences");
        if (!groupBy.isNull())
            extractGroupByPart(groupBy);
        //Extract Having
        JsonNode having = root.path("selectBody").path("having");
        if (!having.isNull())
            extractHaving(having);
        //extract OrderBy
        JsonNode orderby = root.path("selectBody").path("orderByElements");
        if (!orderby.isNull())
            extractOrderBy(orderby);
        //Extract Limit
        JsonNode limitNode = root.path("selectBody").path("limit");
        if (!limitNode.isNull())
            extractLimit(limitNode);

        //Generate basic plan from information
        //we create the plan bootom up output->limit->order...from
        if (limit != null) {

            LimitOperator limitOperator = new LimitOperator(context.getQueryId() + output + ".limit", output, limit);
            try {
                plan.addToCurrent(limitOperator);
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        if (orderByColumns.size() > 0) {
            SortOperator sortOperator = new SortOperator(plan.getCurrent().getName() + ".orderBy", plan.getCurrent().getName(), orderByColumns, ascendingOrder);
            try {
                plan.addToCurrent(sortOperator);
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        if (havingTree != null) {
            FilterOperator filterOperator = new FilterOperator(plan.getCurrent().getName() + ".filter-having", plan.getCurrent().getName(), havingTree);
            try {
                plan.addToCurrent(filterOperator);
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        if (columns.size() > 0 || allTable.size() > 0 || functions.size() > 0) {
            //if we read from only one table and we want all the columns of that table then no reason for projection.
            if (!(allTable.size() == 1)) {
                ProjectOperator project = new ProjectOperator(plan.getCurrent().getName() + ".project", plan.getCurrent().getName(), columns, allTable);
                if (functions.size() > 0) {
                    for (Function f : functions) {
                        Column c = new Column(fromTable, f.toString());
                        project.getColumns().add(c);
                    }
                }
                try {
                    plan.addToCurrent(project);
                } catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
        if ((groupByColumns.size() > 0) || (functions.size() > 0)) {
            GroupByOperator groupby = new GroupByOperator(plan.getCurrent().getName() + ".groupby", plan.getCurrent().getName(), groupByColumns, functions);
            try {
                plan.addToCurrent(groupby);
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        if (whereTree != null) {
            FilterOperator filterOperator = new FilterOperator(plan.getCurrent().getName() + ".filter-where", plan.getCurrent().getName(), whereTree);
            try {
                plan.addToCurrent(filterOperator);
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        //joinTree != null if there are joins then the input will be from the output of joins otherwise we just read the from table
        //else{
        String joinOperatorName = null;
        if (joinTable != null) {
            JoinOperator join = new JoinOperator(plan.getCurrent().getName() + ".join", plan.getCurrent().getName(), joinTable, fromTable, joinColumnB, joinColumnA);
            try {
                plan.addToCurrent(join);
                joinOperatorName = join.getName();
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }

        ReadOperator fromOperator = new ReadOperator(plan.getCurrent().getName() + ".read", plan.getCurrent().getName(), fromTable);
        try {
            plan.addToCurrent(fromOperator);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        //}

        if (joinOperatorName != null) {
            ReadOperator otherTable = new ReadOperator(joinOperatorName + ".readOther", joinOperatorName, joinTable);
            try {
                plan.addTo(joinOperatorName, otherTable);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        plan.computeSources();
        return plan;
    }

    private void extractOrderBy(JsonNode orderby) {

        Iterator<JsonNode> iterator = orderby.iterator();
        try {
            while (iterator.hasNext()) {
                JsonNode expr = iterator.next();
                ascendingOrder.add(expr.path("asc").asBoolean());
                if (expr.has("expression")) {
                    expr = expr.path("expression");
                    if (expr.has("columnName")) {

                        Column selected = mapper.readValue(expr.toString(), Column.class);
                        resolveTableName(selected);
                        orderByColumns.add(selected);
                    } else {
                        Function f = SQLUtils.extractFunction(expr);
                        Column c = new Column(null, f.toString());
                        orderByColumns.add(c);
                    }

                }
            }
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void resolveTableName(Column column) {
        if (column.getTable().getName() == null || column.getTable().getName().equals("")) {
//            System.out.println("Inside");
            String tablename = context.resolveTableName(column.getColumnName());
            column.setTable(new Table(null, tablename));
//            return column;
        }
//        return column;
    }

    private void extractWherePart(JsonNode where) {
        whereTree = new MathOperatorTree(where);
    }

    private void extractSelectItemsPart(JsonNode selectColumns) {

        Iterator<JsonNode> items = selectColumns.iterator();

        try {
            while (items.hasNext()) {
                JsonNode expr = items.next();

                if (expr.has("expression")) {
                    expr = expr.path("expression");
                    if (expr.has("columnName")) {
                        if (allColumns)
                            continue;
                        Column selected = mapper.readValue(expr.toString(), Column.class);
                        resolveTableName(selected);
                        columns.add(selected);
                    } else {

                        expr.findParent("expression");
                        Function selected = SQLUtils.extractFunction(expr);
                        functions.add(selected);
                    }
                } else {
                    if (!expr.has("table")) {
                        columns.clear();
                        allColumns = true;
                    } else {
                        expr = expr.path("table");
                        Table table = mapper.readValue(expr.toString(), Table.class);
                        allTable.add(table);
                    }
                }
            }
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void extractGroupByPart(JsonNode groupBy) {

        Iterator<JsonNode> items = groupBy.iterator();

        try {
            while (items.hasNext()) {
                JsonNode expr = items.next();
                Column selected = mapper.readValue(expr.toString(), Column.class);
                resolveTableName(selected);
                groupByColumns.add(selected);
            }
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void extractHaving(JsonNode having) {

        havingTree = new MathOperatorTree(having);
    }

    private void extractLimit(JsonNode limitNode) {

        try {
            limit = mapper.readValue(limitNode.toString(), Limit.class);

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    private void extractJoinPart(JsonNode joins) {

        if (!joins.isNull()) {
            Iterator<JsonNode> iterator = joins.elements();
            while (iterator.hasNext()) {
                JsonNode joinRoot = iterator.next();
                try {
                    joinTable = mapper.readValue(joinRoot.path("rightItem").toString(), Table.class);
                    joinColumnA = mapper.readValue(joinRoot.path("onExpression").path("leftExpression").toString(), Column.class);
                    joinColumnB = mapper.readValue(joinRoot.path("onExpression").path("rightExpression").toString(), Column.class);
                    context.addTable(joinTable);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private void extractFromPart(JsonNode fromNode) {

        if (fromNode.get("selectBody") != null) {
            JsonNode json = fromNode.path("selectBody");
            BasicPlannerExtractor subSelect = new SelectExtractor(json);
//            Plan subselectPlan = subSelect.extractPlan("subSelect->" + output, context);
        } else {
            try {
                fromTable = mapper.readValue(fromNode.toString(), Table.class);
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            assert (fromTable != null);
            context.addTable(fromTable);
        }

    }
}
