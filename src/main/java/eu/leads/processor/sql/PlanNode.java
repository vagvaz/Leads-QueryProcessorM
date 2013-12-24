package eu.leads.processor.sql;

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/2/13
 * Time: 9:54 AM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class PlanNode {
    protected String type = "default";
    protected String name = "";
    @JsonIgnore
    private Properties configuration;
    protected List<String> sources;
    protected String output;

    @JsonCreator
    public PlanNode(@JsonProperty("name") String name, @JsonProperty("type") String type, @JsonProperty("output") String output, @JsonProperty("sources") List<String> sources) {
        this.name = name;
        this.type = type;
        this.output = output;
        this.sources = new ArrayList<String>(sources);
    }

    public PlanNode(String name) {
        this.name = name;
        configuration = new Properties();
        sources = new ArrayList<String>();
    }

    public PlanNode(PlanNode node) {
        name = node.getName();
        configuration = new Properties();
        configuration.putAll((Map<?, ?>) node.getConfiguration());
        sources.addAll(node.getSources());
        output = node.getOutput();
        type = node.getType();

    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonIgnore
    public Properties getConfiguration() {
        return configuration;
    }

    @JsonIgnore
    public void setConfiguration(Properties configuration) {
        this.configuration = configuration;
    }

    public List<String> getSources() {
        return sources;
    }

    public void setSources(List<String> sources) {
        this.sources = sources;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    @JsonIgnore
    public void addParameter(String parameterKey, String value) {
        configuration.setProperty(parameterKey, value);
    }

    @JsonIgnore
    public String getParameter(String parameterKey) {
        String result = configuration.getProperty(parameterKey);
        return Strings.nullToEmpty(result);
    }
}
