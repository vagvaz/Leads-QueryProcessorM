package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import eu.leads.processor.plan.ExecutionPlanNode;
import eu.leads.processor.sql.PlanNode;
import net.sf.jsqlparser.schema.Column;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 1:12 AM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect
public class SortOperator extends ExecutionPlanNode {
    List<Column> columns;

    public List<Boolean> getAscending() {
        return ascending;
    }

    public void setAscending(List<Boolean> ascending) {
        this.ascending = ascending;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    List<Boolean> ascending;

    public SortOperator(String name) {
        super(name, OperatorType.SORT);
    }

    public SortOperator(PlanNode node) {
        super(node, OperatorType.SORT);
    }

    @JsonCreator
    public SortOperator(@JsonProperty("name") String name, @JsonProperty("output") String output, @JsonProperty("columns") List<Column> orderByColumns, @JsonProperty("asceding") List<Boolean> ascendingOrder) {
        super(name, OperatorType.SORT);
        setOutput(output);
        this.columns = orderByColumns;
        this.ascending = ascendingOrder;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(" ");
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getTable() != null)
                builder.append(columns.get(i).getWholeColumnName() + " " + (ascending.get(i) ? " ASC " : " DESC "));
            else
                builder.append(columns.get(i).getColumnName() + " " + (ascending.get(i) ? " ASC " : " DESC "));
        }
        return getType() + builder.toString();
    }
}
