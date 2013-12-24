package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import eu.leads.processor.plan.ExecutionPlanNode;
import eu.leads.processor.sql.PlanNode;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/5/13
 * Time: 3:56 PM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect
public class OutputOperator extends ExecutionPlanNode {

    @JsonCreator
    public OutputOperator(@JsonProperty("name") String name) {
        super(name, OperatorType.OUTPUT);

    }

    public OutputOperator(PlanNode node) {
        super(node, OperatorType.OUTPUT);
    }

    @Override
    public String toString() {
        return "OUTPUT";
    }
}
