package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import eu.leads.processor.plan.ExecutionPlanNode;
import eu.leads.processor.sql.PlanNode;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/7/13
 * Time: 8:34 AM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect
public class JoinOperator extends ExecutionPlanNode {

    Table left;
    Table right;
    Column leftColumn;
    Column rightColumn;


    @JsonCreator
    public JoinOperator(@JsonProperty("name") String name, @JsonProperty("output") String output, @JsonProperty("left") Table leftTable, @JsonProperty("right") Table rightTable, @JsonProperty("leftColumn") Column leftColumn, @JsonProperty("rightColumn") Column rightColumn) {
        super(name, OperatorType.JOIN);
        this.left = leftTable;
        this.right = rightTable;
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
        setOutput(output);
        setOperatorType(OperatorType.JOIN);
        setType(OperatorType.toString(OperatorType.JOIN));
    }

    public Table getLeft() {
        return left;
    }

    public void setLeft(Table left) {
        this.left = left;
    }

    public Table getRight() {
        return right;
    }

    public void setRight(Table right) {
        this.right = right;
    }

    public Column getLeftColumn() {
        return leftColumn;
    }

    public void setLeftColumn(Column leftColumn) {
        this.leftColumn = leftColumn;
    }

    public Column getRightColumn() {
        return rightColumn;
    }

    public void setRightColumn(Column rightColumn) {
        this.rightColumn = rightColumn;
    }

    public JoinOperator(String name) {
        super(name);
    }


//    public JoinOperator(@JsonProperty("name") String name, @JsonProperty("operatorType") OperatorType type,@JsonProperty("output") String output, Table right, Table left, Column rightColumn,Column leftColumn) {
//        super(name, OperatorType.JOIN);
//        this.left = left;
//        this.right = right;
//        this.leftColumn = leftColumn;
//        this.rightColumn = rightColumn;
//        setOutput(output);
//        setOperatorType(OperatorType.JOIN);
//        setType(OperatorType.toString(OperatorType.JOIN));
//    }

    public JoinOperator(PlanNode node) {
        super(node, OperatorType.JOIN);
    }

    @Override
    public String toString() {
        return getType() + " " + left.getName() + "." + leftColumn.getColumnName() + " = " + right.getName() + "." + rightColumn.getColumnName();
    }
}
