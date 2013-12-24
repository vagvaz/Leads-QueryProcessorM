package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import eu.leads.processor.plan.ExecutionPlanNode;
import eu.leads.processor.sql.PlanNode;
import net.sf.jsqlparser.schema.Table;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/25/13
 * Time: 11:15 AM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect
public class ReadOperator extends ExecutionPlanNode {
    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    private Table table;

    public ReadOperator(String name) {
        super(name, OperatorType.READ);
    }

    public ReadOperator(PlanNode node) {
        super(node, OperatorType.READ);
    }

    @JsonCreator
    public ReadOperator(@JsonProperty("name") String name, @JsonProperty("output") String output, @JsonProperty("table") Table table) {
        super(name, OperatorType.READ);
        setOutput(output);
        this.table = table;

    }

    @Override
    public String toString() {
        return getType() + " " + table.getWholeTableName();
    }
//    public ReadOperator()
}
