package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import eu.leads.processor.plan.ExecutionPlanNode;
import eu.leads.processor.sql.PlanNode;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 1:19 AM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect
@JsonDeserialize(converter = GroupByJsonDelegate.class)
public class GroupByOperator extends ExecutionPlanNode {


    private List<Column> columns;
    private List<Function> functions;

    @JsonCreator
    public GroupByOperator(@JsonProperty("name") String name, @JsonProperty("output") String output, @JsonProperty("columns") List<Column> groupByColumns, @JsonProperty("functions") List<Function> functions) {
        super(name, OperatorType.GROUPBY);
        this.columns = groupByColumns;
        this.functions = functions;
        setOutput(output);
        setOperatorType(OperatorType.GROUPBY);
        setType(OperatorType.toString(OperatorType.GROUPBY));
    }

    public GroupByOperator(String name) {
        super(name);
        setOutput(output);
        setOperatorType(OperatorType.GROUPBY);
        setType(OperatorType.toString(OperatorType.GROUPBY));
    }

    public GroupByOperator(PlanNode node) {
        super(node, OperatorType.GROUPBY);
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<Function> getFunctions() {
        return functions;
    }

    public void setFunctions(List<Function> functions) {
        this.functions = functions;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(" ");
        for (Column c : columns) {
            builder.append(c.getWholeColumnName() + "\t");
        }
        if (functions.size() > 0) {
            builder.append(" computing functions ");
            for (Function f : functions)
                builder.append(f.toString() + ", ");
        }
        return getType() + builder.toString();
    }
}
