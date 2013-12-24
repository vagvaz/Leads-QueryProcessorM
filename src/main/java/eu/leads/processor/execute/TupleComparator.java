package eu.leads.processor.execute;

import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 8:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class TupleComparator implements Comparator<Tuple> {
    final List<Column> columns;
    final ArrayList<Integer> sign;
    final ArrayList<Boolean> arithmetic;

    public TupleComparator(List<Column> columns, List<Boolean> ascending, List<Boolean> arithmetic) {
        this.columns = new ArrayList<Column>(columns);

        this.arithmetic = new ArrayList<Boolean>(arithmetic);
        this.sign = new ArrayList<Integer>();
        for (Boolean asc : ascending) {
            if (asc)
                sign.add(1);
            else
                sign.add(-1);
        }
    }

    @Override
    public int compare(Tuple o1, Tuple o2) {
        int index = 0;
        for (Column col : columns) {
            if (!o1.getAttribute(col.getColumnName()).equals(o2.getAttribute(col.getColumnName()))) {
                if (arithmetic.get(index)) {
                    int result = compareNumbers(o1.getAttribute(col.getColumnName()), o2.getAttribute(col.getColumnName()));
                    return result * sign.get(index);
                } else {
                    int result = o1.getAttribute(col.getColumnName()).compareTo(o2.getAttribute(col.getColumnName()));
                    return result * sign.get(index);
                }
            }
            index++;
        }
        return 0;
    }

    private int compareNumbers(String op1, String op2) {
        Double o1 = Double.valueOf(op1);
        Double o2 = Double.valueOf(op2);
        return o1.compareTo(o2);
    }

    private int compareStrings(String op1, String op2) {
        return op1.compareTo(op2);
    }
}
