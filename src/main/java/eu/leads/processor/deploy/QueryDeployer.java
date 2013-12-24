package eu.leads.processor.deploy;

import eu.leads.processor.execute.operators.Operator;
import eu.leads.processor.query.Query;
import eu.leads.processor.query.QueryState;
import eu.leads.processor.sql.Plan;

public interface QueryDeployer {
    void execute(Plan p);

    QueryState getStatus(Query q);

    void complete(Operator op, Plan p);

    void recover(Plan p, Query q);
}
