package eu.leads.processor.query;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import eu.leads.processor.sql.Plan;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/9/13
 * Time: 1:06 AM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect
public class SQLQuery implements Query {
    private String user;
    private String id;
    private boolean completed;
    private String queryText;
    private String location;
    private String sqlType; //possible (SELECT,INSERT,DELETE,CREATE TABLE)
    private Plan sqlPlan;
    private QueryContext context;
    private QueryState state;

    public Plan getSqlPlan() {
        //  if(sqlPlan==null)
        //      sqlPlan = getBasicPlan(this.queryText,sqlType);
        return sqlPlan;
    }

    public void setSqlPlan(Plan sqlPlan) {
        this.sqlPlan = sqlPlan;
    }

    public SQLQuery(String user, String location, String query, String sqlType) {
        this.user = user;
        this.location = location;
        this.queryText = query;
        this.sqlType = sqlType;
        this.sqlPlan = null;
    }

    @Override
    public void setUser(String user) {
        this.user = user;
    }

    @Override
    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public String getQueryText() {
        return queryText;
    }

    public void setQueryText(String queryText) {
        this.queryText = queryText;
    }

    @Override
    public String getLocation() {
        return location;
    }

    @Override
    public Plan getPlan() {
        return getSqlPlan();  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setPlan(Plan plan) {
        this.sqlPlan = plan;
    }

    @Override
    public QueryContext getContext() {
        return context;
    }

    @Override
    public void setQueryContext(QueryContext context) {
        this.context = context;
        this.context.setQueryId(this.getId());
    }

    @Override
    public QueryState getQueryState() {
        return state;
    }

    @Override
    public void setQueryState(QueryState state) {
        this.state = state;
    }

    @Override
    public void setLocation(String location) {
        this.location = location;
    }

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    @Override
    public String getUser() {
        return user;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getId() {
        return id;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isCompleted() {
        return completed;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

}
