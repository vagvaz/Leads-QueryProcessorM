package eu.leads.processor.execute.operators;

import java.util.Properties;

public class OperatorConfiguration {
    Properties configuration = null;

    public OperatorConfiguration(Properties configuration) {
        this.configuration = configuration;
    }

    public Object getOutputTable() {
        return configuration.get("output");
    }

    public void setOutputTable(String outputTable) {
        configuration.put("output", outputTable);
    }

    public OperatorSubType getSubType() {
        return OperatorSubType.fromString(configuration.get("operatora.subtype"));
    }

    public void setSubType(OperatorSubType subtype) {
        configuration.put("operatora.subtype", OperatorSubType.toString(subtype));
    }

//    public OperatorType geType() {
//        return OperatorType.NONE;
//    }

    public void setType(OperatorType operator) {
        configuration.put("operator.type", OperatorType.toString(operator));
    }

    public Object getParameter(String key) {
        return configuration.get(key);
    }

    public void setParameter(String key, String value) {
        configuration.put(key, value);
    }

}
