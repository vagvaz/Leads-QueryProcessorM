package eu.leads.processor.plan;

import eu.leads.processor.query.QueryContext;
import eu.leads.processor.sql.Plan;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/21/13
 * Time: 12:23 PM
 * To change this template use File | Settings | File Templates.
 */
public interface BasicPlannerExtractor {
    public Plan extractPlan(String ouputNode, QueryContext context);
}
