package eu.leads.processor.query;

import eu.leads.processor.sql.Plan;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/9/13
 * Time: 1:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class WorkflowQuery implements Query {
    @Override
    public String getUser() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setUser(String user) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getId() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isCompleted() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setCompleted(boolean complete) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setId(String id) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getLocation() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setLocation(String location) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Plan getPlan() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setPlan(Plan paln) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public QueryContext getContext() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setQueryContext(QueryContext context) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public QueryState getQueryState() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public void setQueryState(QueryState state) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
