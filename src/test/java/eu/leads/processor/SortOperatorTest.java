package eu.leads.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.execute.operators.SortOperator;
import eu.leads.processor.execute.operators.SortOperatorImplementation;
import eu.leads.processor.query.SQLQuery;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.Utilities;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.infinispan.Cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/5/13
 * Time: 12:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class SortOperatorTest {
    private static final int numOfTuples = 10;
    private static final String[] columnNames = {"url", "domain", "pagerank", "body", "sentiment"};
    private static final String[] columnType = {"string", "string", "double", "string", "double"};


    public static void main(String[] args) {
        InfinispanUtils.start();

//        ObjectMapper mapper = new ObjectMapper();
        try {
//            SQLQuery query = new SQLQuery("user", "location", "select url,domain from webpages", "SELECT");
            Cache<String, String> inputMap = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap("testData");
            for (int i = 0; i < numOfTuples; i++) {
                Tuple t = Utilities.generateTuple(columnNames, columnType);
                inputMap.put("test:" + (Integer.toString(i)), t.asString());
            }
            System.out.println("intial");
            Utilities.printMap(inputMap);
            Table t = new Table("schema", "webpages");
            List<Column> c = new ArrayList<Column>();
            List<Boolean> ascending = new ArrayList<Boolean>();
            Column urlColumn = new Column(t, "url");
            ascending.add(false);
            Column pagerank = new Column(t, "pagerank");
            ascending.add(true);
            c.add(urlColumn);
            c.add(pagerank);
            SortOperator sort = new SortOperator("sort", "output", c, ascending);
            SortOperatorImplementation op = new SortOperatorImplementation("testData", "sort", sort, null);
            op.execute();
            System.out.println("sorted");
            Map<String, String> dataMap = InfinispanUtils.getOrCreatePersistentMap("data");
            Utilities.printMap(dataMap);
            System.out.println("goodbye");
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
