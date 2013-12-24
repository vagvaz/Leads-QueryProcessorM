package eu.leads.processor.plan;

import eu.leads.processor.query.Query;
import eu.leads.processor.sql.Plan;

import java.util.SortedMap;

public interface QueryPlanner {
    SortedMap generatePlans(Query q);

    Plan choosePlan(SortedMap<Double, Plan> plans);

}
