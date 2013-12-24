package eu.leads.processor.plan;

import com.fasterxml.jackson.annotation.*;
import eu.leads.processor.execute.operators.OperatorType;
import eu.leads.processor.sql.PlanNode;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/22/13
 * Time: 7:12 PM
 * To change this template use File | Settings | File Templates.
 */

//Node for the Execution plan
@JsonAutoDetect
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public class ExecutionPlanNode extends PlanNode {
    @JsonIgnore
    Properties conf;
    OperatorType operatorType;
    NodeStatus status = NodeStatus.PENDING;

    public OperatorType getOperatorType() {
        return operatorType;
    }

    public void setOperatorType(OperatorType operatorType) {
        this.operatorType = operatorType;
    }

    public ExecutionPlanNode(String name) {
        super(name);

    }

    @JsonCreator
    public ExecutionPlanNode(@JsonProperty("name") String name, @JsonProperty("type") OperatorType type) {
        super(name);
//        conf = new OperatorConfiguration();
        this.operatorType = type;
        setType(OperatorType.toString(type));
    }

    public ExecutionPlanNode(PlanNode node, OperatorType type) {
        super(node);
//        conf = new OperatorConfiguration((Properties)super.getConfiguration());
//        conf.setType(type);

        this.operatorType = type;
    }

    @Override
    public Properties getConfiguration() {

        return conf;  //To change body of implemented methods use File | Settings | File Templates.

    }

    @Override
    public void setConfiguration(Properties config) {
        conf = config;
    }

    @Override
    public String getType() {
        return OperatorType.toString(this.operatorType);
    }

    @Override
    public void setType(String type) {
        this.operatorType = OperatorType.fromString(type);
    }

    public NodeStatus getStatus() {
        return status;
    }

    public void setStatus(NodeStatus status) {
        this.status = status;
    }

    @JsonIgnore
    public OperatorType type() {
        return OperatorType.fromString(getType());
    }

    @Override
    public String toString() {
        return "ExecutionPlanNode " + getType();
    }

}
