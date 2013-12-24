package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import eu.leads.processor.plan.ExecutionPlanNode;
import eu.leads.processor.sql.PlanNode;
import eu.leads.processor.utils.math.MathOperatorTree;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 7:06 AM
 * To change this template use File | Settings | File Templates.
 */
//Filter Operator
public class FilterOperator extends ExecutionPlanNode {


    private MathOperatorTree tree;

    public FilterOperator(String name) {
        super(name, OperatorType.FILTER);
    }

    public FilterOperator(PlanNode node) {
        super(node, OperatorType.FILTER);
    }

    public MathOperatorTree getTree() {
        return tree;
    }

    public void setTree(MathOperatorTree tree) {
        this.tree = tree;
    }

    @JsonCreator
    public FilterOperator(@JsonProperty("name") String name, @JsonProperty("output") String output, @JsonProperty("tree") MathOperatorTree operatorTree) {
        super(name, OperatorType.FILTER);
        setOutput(output);
        this.tree = operatorTree;
    }

    @Override
    public String toString() {
        return getType() + tree.toString();
    }
}
