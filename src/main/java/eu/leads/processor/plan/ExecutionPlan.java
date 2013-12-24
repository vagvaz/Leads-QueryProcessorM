package eu.leads.processor.plan;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import eu.leads.processor.execute.operators.OutputOperator;

import eu.leads.processor.sql.Plan;
import eu.leads.processor.sql.PlanNode;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/16/13
 * Time: 3:57 PM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect
//@JsonDeserialize(converter = ExecutionPlanConverter.class)
public class ExecutionPlan implements Plan {
    private OutputOperator output;
    private Map<String, ExecutionPlanNode> graph;


    private List<String> sources;
    @JsonIgnore
    private
    PlanNode current;

    @JsonCreator
    public ExecutionPlan(@JsonProperty("output") OutputOperator output, @JsonProperty("graph") Map<String, ExecutionPlanNode> graph, @JsonProperty("sources") List<String> sources) {
        this.output = output;
        this.graph = graph;
        this.sources = sources;
    }

    public ExecutionPlan() {
        graph = new HashMap<String, ExecutionPlanNode>();
        sources = new ArrayList<String>();
    }

    public Map<String, ExecutionPlanNode> getGraph() {
        return graph;
    }

    public void setGraph(Map<String, ExecutionPlanNode> graph) {
        this.graph = graph;
    }

    public void setSources(List<String> sources) {
        this.sources = sources;
    }


    void setOutput(OutputOperator node) {
        output = (node);
        graph.put(node.getName(), output);
        current = output;
    }


    @Override
    public void setOutput(PlanNode node) {
        this.setOutput((OutputOperator) node);
    }

    public OutputOperator getOutput() {
        return output;
    }

    @Override
    public void addSource(PlanNode node) {
        if( node instanceof  ExecutionPlanNode){
        sources.add(node.getName());
        graph.put(node.getName(), (ExecutionPlanNode) node);
        }
    }

    @JsonIgnore
    @Override
    public void addTo(String nodeId, Plan subPlan) throws Exception {
        PlanNode entryPoint = getNode(nodeId);
        if (entryPoint == null)
            throw new Exception("sub plan has invalid connection point to this plan " + nodeId);
        if (entryPoint.getSources().size() == 0) {
            PlanNode subOutput = subPlan.getOutput();
            entryPoint.setSources(subOutput.getSources());
            for (String source : subOutput.getSources()) {
                PlanNode node = subPlan.getNode(source);
                node.setOutput(entryPoint.getName());
            }
        }
        for (PlanNode node : subPlan.getNodes()) {
            if (!node.equals(subPlan.getOutput())) {
                graph.put(node.getName(), (ExecutionPlanNode) node);
            }
        }

        sources.addAll(subPlan.getSources());
    }

    @JsonIgnore
    public PlanNode getNode(String nodeId) {
        return graph.get(nodeId);
    }

    @JsonIgnore
    @Override
    public PlanNode createNode() {
        return null;  //TODO
    }

    @JsonIgnore
    @Override
    public void addTo(String nodeId, PlanNode newNode) throws Exception {
        PlanNode node = getNode(nodeId);
        if (node == null)
            throw new Exception("sub plan has invalid connection point to this plan " + nodeId);
        node.getSources().add(newNode.getName());
        newNode.setOutput(node.getName());
        graph.put(newNode.getName(), (ExecutionPlanNode) newNode);
    }

    @JsonIgnore
    public void addAfter() {
        //TODO
    }

    @JsonIgnore
    @Override
    public void addAfterCurrent(PlanNode node) throws Exception {
        this.addAfter();
        setCurrent(node);
    }

    @JsonIgnore
    @Override
    public void addAfterCurrent() {
        //TODO
    }

    @JsonIgnore
    @Override
    public void addToCurrent(PlanNode node) throws Exception {
        try {
            this.addTo(getCurrent().getName(), node);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        setCurrent(node);
    }

    @JsonIgnore
    @Override
    public void addToCurrent() {
        //TODO
    }

    @Override
    public Collection<String> getSources() {
        return sources;
    }

    @JsonIgnore
    @Override
    public void computeSources() {
        sources.clear();
        for (Map.Entry<String, ExecutionPlanNode> entry : graph.entrySet()) {
            if (entry.getValue().getSources().size() == 0)
                sources.add(entry.getValue().getName());
        }
    }

    @JsonIgnore
    @Override
    public void merge(Plan extracted) throws Exception {
        addTo(extracted.getOutput().getName(), extracted);
    }

    @JsonIgnore
    @Override
    public Collection<PlanNode> getNodes() {
        ArrayList<PlanNode> result = new ArrayList<PlanNode>();
        result.addAll(graph.values());
        return result;
    }

    @JsonIgnore
    @Override
    public PlanNode getCurrent() {
        return current;
    }

    @JsonIgnore
    @Override
    public void setCurrent(String nodeId) throws Exception {
        PlanNode tmp = graph.get(nodeId);
        if (tmp != null)
            current = tmp;
        else
            throw new Exception("Setting Current to invalid node " + nodeId);
    }

    @JsonIgnore
    @Override
    public void setCurrent(PlanNode node) throws Exception {
        PlanNode tmp = graph.get(node.getName());
        if (tmp != null)
            current = tmp;
        else
            throw new Exception("Setting Current to invalid node " + node.getName());
    }

    @Override
    public String toString() {

        Set<PlanNode> current = new HashSet<PlanNode>();
        for (String source : sources)
            current.add(graph.get(source));
        Set<PlanNode> next = new HashSet<PlanNode>();
        StringBuilder builder = new StringBuilder();
        while (current.size() > 0) {
            for (PlanNode node : current) {
                builder.append(node.toString() + "\t");
                if (node.getName().equals(output.getName()))
                    break;
                if (!Strings.isNullOrEmpty(node.getOutput()))
                    next.add(graph.get(node.getOutput()));
            }
            builder.append("\n");
            current.clear();
            current = next;
            next = new HashSet<PlanNode>();
        }

        return builder.toString();
    }
}
