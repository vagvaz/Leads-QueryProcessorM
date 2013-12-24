package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import eu.leads.processor.plan.ExecutionPlanNode;
import eu.leads.processor.sql.PlanNode;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 1:26 AM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect
public class ProjectOperator extends ExecutionPlanNode {
    private List<Column> columns;
    private List<Table> tables;

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    @JsonCreator
    public ProjectOperator(@JsonProperty("name") String name, @JsonProperty("output") String output, @JsonProperty("columns") List<Column> columns, @JsonProperty("tables") List<Table> allTable) {
        super(name, OperatorType.PROJECT);
        this.columns = columns;
        this.tables = allTable;
        setOutput(output);
    }

    public ProjectOperator(String name) {
        super(name, OperatorType.PROJECT);
    }

    public ProjectOperator(PlanNode node) {
        super(node, OperatorType.PROJECT);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(" ");
        for (Column c : columns) {
            builder.append(c.getWholeColumnName() + " , ");
        }
        for (Table t : tables)
            builder.append(t.getWholeTableName());

        return getType() + builder.toString();
    }
}
