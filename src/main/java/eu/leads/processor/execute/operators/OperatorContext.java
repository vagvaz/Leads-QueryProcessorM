package eu.leads.processor.execute.operators;

import eu.leads.processor.query.Query;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/22/13
 * Time: 6:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class OperatorContext {
    String user;
    Query query;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }
}
