package eu.leads.processor.sql.select;

import eu.leads.processor.sql.Plan;
import eu.leads.processor.sql.PlanNode;
import eu.leads.processor.sql.PlanTraverser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/3/13
 * Time: 10:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class LeadsSelectVisitor implements SelectVisitor, SelectItemVisitor {
    private final Plan plan;
    private final PlanTraverser traverser;
    static final Logger log = Logger.getLogger(LeadsSelectVisitor.class.getName());

    public LeadsSelectVisitor(Plan plan, PlanTraverser traverser) {
        this.plan = plan;
        this.traverser = traverser;
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        //First check the limit of the query
        Limit limit = plainSelect.getLimit();

        if (limit != null) {
            PlanNode limitNode;
//            limitNode = plan.createNodeWithOutput(traverser.getCurrent().getName());
//            limitNode.addParameter("offset", Long.toString(limit.getOffset()));
//            limitNode.addParameter("rowCount", Long.toString(limit.getRowCount()));
//            limitNode.addParameter("limitAll", Boolean.toString(limit.isLimitAll()));
//            traverser.goTo(limitNode.getName());
        }
        List<OrderByElement> orderby = plainSelect.getOrderByElements();
        if (orderby != null) {
            for (Iterator<OrderByElement> iterator = orderby.iterator(); iterator.hasNext(); ) {
                OrderByElement next = iterator.next();
                if (next.getExpression() instanceof Column) {

                } else {
                    log.error("ORDER BY not coumn but " + next.getExpression().toString());
                }

            }
        }

// We will not check the         plainSelect.getTop();

        plainSelect.getGroupByColumnReferences();
        plainSelect.getFromItem();
        plainSelect.getJoins();
        plainSelect.getHaving();
        plainSelect.getWhere();
        plainSelect.getDistinct();
        plainSelect.getSelectItems();
        plainSelect.getInto();

    }

    @Override
    public void visit(SetOperationList setOpList) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(WithItem withItem) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(AllColumns allColumns) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
