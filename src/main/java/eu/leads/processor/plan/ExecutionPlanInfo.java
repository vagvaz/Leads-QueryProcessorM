package eu.leads.processor.plan;

import eu.leads.processor.execute.operators.OperatorType;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.sql.PlanNode;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.listeners.PrefixListener;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/30/13
 * Time: 12:05 PM
 * To change this template use File | Settings | File Templates.
 */
//Class that keeps all the relevant information for the execution plan in LeadsDeployerService.
public class ExecutionPlanInfo {
    private ExecutionPlan plan;
    private QueryContext context;
    private final ArrayList<Object> listeners;


    public ExecutionPlanInfo(ExecutionPlan plan, QueryContext context) {
        this.plan = plan;
        this.context = context;

        listeners = new ArrayList<Object>();

    }

    public ExecutionPlan getPlan() {
        return plan;
    }

    public void setPlan(ExecutionPlan plan) {
        this.plan = plan;
    }

    public QueryContext getContext() {
        return context;
    }

    public void setContext(QueryContext context) {
        this.context = context;
    }

    public void addListener(PrefixListener listener) {
        listeners.add(listener);
    }

    public void finalizePlan() {
        for (PlanNode node : this.plan.getNodes()) {
            ExecutionPlanNode n = (ExecutionPlanNode) node;
            if(((ExecutionPlanNode) node).getOperatorType().equals(OperatorType.READ))
                continue;
            if(this.plan.getOutput().getName().equals(n.getOutput()))
                continue;
            InfinispanUtils.removeCache(n.getName()+":");
        }
    }
}
