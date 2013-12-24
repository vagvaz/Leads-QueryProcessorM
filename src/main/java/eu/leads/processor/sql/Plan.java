package eu.leads.processor.sql;

import java.util.Collection;

public interface Plan {

    public void setOutput(PlanNode node);

    public PlanNode getOutput();

    public void addSource(PlanNode node);

    public void addTo(String node, Plan subPlan) throws Exception;

    public void addTo(String node, PlanNode subnode) throws Exception;

    public void addAfter();

    public void addAfterCurrent(PlanNode node) throws Exception;

    public void addAfterCurrent();

    public void addToCurrent(PlanNode node) throws Exception;

    public void addToCurrent();


    public void merge(Plan extracted) throws Exception;

    public Collection<PlanNode> getNodes();

    public PlanNode getCurrent();

    public void setCurrent(String nodeId) throws Exception;

    public void setCurrent(PlanNode node) throws Exception;

    public PlanNode getNode(String nodeId);

    public PlanNode createNode();

    public Collection<String> getSources();

    public void computeSources();
}
