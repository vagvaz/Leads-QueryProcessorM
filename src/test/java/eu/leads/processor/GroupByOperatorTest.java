package eu.leads.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.execute.operators.GroupByMapper;
import eu.leads.processor.execute.operators.GroupByReducer;
import eu.leads.processor.query.SQLQuery;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.Utilities;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.MapReduceTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/5/13
 * Time: 12:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class GroupByOperatorTest {
    private static final int numOfTuples = 10;
    private static final String[] columnNames = {"url", "domain", "pagerank", "body", "sentiment"};
    private static final String[] columnType = {"string", "string", "int", "string", "double"};


    public static void main(String[] args) {
        InfinispanUtils.start();

        ObjectMapper mapper = new ObjectMapper();
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
            List<Expression> c = new ArrayList<Expression>();

//            Column urlColumn = new Column(t, "url");

            Column pagerank = new Column(t, "pagerank");

            c.add(pagerank);
            //c.add(pagerank);
            ArrayList<Function> functions = new ArrayList<Function>();
            Function f = new Function();
            f.setName("AVG");
            functions.add(f);
            ExpressionList exp = new ExpressionList();
            exp.setExpressions(c);
            f.setParameters(exp);
            MapReduceTask<String, String, String, String> task = new MapReduceTask<String, String, String, String>(inputMap);
            Properties conf = new Properties();
            conf.setProperty("output", "groupby");
            conf.setProperty("functions", mapper.writeValueAsString(functions));
            System.out.println(mapper.writeValueAsString(functions));
            conf.setProperty("columns", "url");
            task.mappedWith(new GroupByMapper(conf)).reducedWith(new GroupByReducer(conf));
            task.execute();
            System.out.println("groupby");
            Map<String, String> dataMap = InfinispanUtils.getOrCreatePersistentMap("data");
            Utilities.printMap(dataMap);
            System.out.println("goodbye");
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
