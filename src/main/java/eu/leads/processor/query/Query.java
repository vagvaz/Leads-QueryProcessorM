package eu.leads.processor.query;

import eu.leads.processor.sql.Plan;

enum QueryType {SQL, WORKFLOW}

public interface Query {
    QueryType type = QueryType.SQL;

    String getUser();

    void setUser(String user);

    String getId();


    boolean isCompleted();

    void setCompleted(boolean complete);

    void setId(String id);

    String getLocation();

    void setLocation(String location);

    Plan getPlan();

    void setPlan(Plan plan);

    QueryContext getContext();

    void setQueryContext(QueryContext context);

    QueryState getQueryState();

    void setQueryState(QueryState state);
}
