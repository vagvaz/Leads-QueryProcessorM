package eu.leads.processor.execute.operators;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/30/13
 * Time: 10:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class OperatorCompletionEvent {
    private String operator;
    private String queryId;

    public OperatorCompletionEvent(String operator, String id) {
        this.operator = operator;
        queryId = id;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
}
