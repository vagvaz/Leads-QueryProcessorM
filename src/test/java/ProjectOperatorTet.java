import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.LeadsReducer;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.execute.operators.ProjectMapper;
import eu.leads.processor.query.SQLQuery;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.Utilities;
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
 * Date: 11/4/13
 * Time: 1:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class ProjectOperatorTet {
    private static final int numOfTuples = 10;
    private static final String[] columnNames = {"url", "domain", "pagerank", "body", "sentiment"};
    private static final String[] columnType = {"string", "string", "double", "string", "double"};


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
            List<Column> c = new ArrayList<Column>();
            Column urlColumn = new Column(t, "url");
            Column pagerank = new Column(t, "pagerank");
            c.add(urlColumn);
            c.add(pagerank);
            MapReduceTask<String, String, String, String> task = new MapReduceTask<String, String, String, String>(inputMap);
            Properties conf = new Properties();
            conf.setProperty("output", "testOutput");
            conf.setProperty("columns", mapper.writeValueAsString(c));
            conf.setProperty("data", "data");
            task.mappedWith(new ProjectMapper(conf)).reducedWith(new LeadsReducer<String, String>(conf));
//            Map<String, String> result = task.execute();

            System.out.println("projected");
            Map<String, String> dataMap = InfinispanUtils.getOrCreatePersistentMap("data");
            Utilities.printMap(dataMap);
            System.out.println("goodbye");
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
