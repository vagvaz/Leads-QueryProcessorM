package eu.leads.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.execute.operators.LimitOperatorImplemetation;
import eu.leads.processor.query.SQLQuery;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.Utilities;
import net.sf.jsqlparser.statement.select.Limit;
import org.infinispan.Cache;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/5/13
 * Time: 12:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class LimitOperatorTest {
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
            Limit limit = new Limit();
            limit.setRowCount(3);
            LimitOperatorImplemetation op = new LimitOperatorImplemetation("testData", "limit", limit, false);
            op.execute();
            System.out.println("limited");
            Map<String, String> dataMap = InfinispanUtils.getOrCreatePersistentMap("data");
            Utilities.printMap(dataMap);
            System.out.println("goodbye");
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
