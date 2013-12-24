package eu.leads.processor.execute.operators;


import java.util.Properties;

public interface Operator {
    public Properties getConfiguration();

    public void setConfiguration(Properties config);
}
