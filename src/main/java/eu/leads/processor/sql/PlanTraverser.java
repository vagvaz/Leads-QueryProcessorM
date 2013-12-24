package eu.leads.processor.sql;

import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/3/13
 * Time: 9:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class PlanTraverser {
    private Plan plan;
    private List<PlanNode> nodeStack;
    private PlanNode current;

    public PlanTraverser(Plan plan) {
        this.plan = plan;
        nodeStack = new LinkedList<PlanNode>();
        current = null;
    }

    public Plan getPlan() {
        return plan;
    }

    public void setPlan(Plan plan) {
        this.plan = plan;
    }

    public List<PlanNode> getNodeStack() {
        return nodeStack;
    }

    public void setNodeStack(List<PlanNode> nodeStack) {
        this.nodeStack = nodeStack;
    }

    public PlanNode getCurrent() {
        return current;
    }

    public void setCurrent(PlanNode current) {
        this.current = current;
    }

}
