import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.LeadsReducer;
import eu.leads.processor.execute.TableInfo;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.execute.operators.FilterOperator;
import eu.leads.processor.execute.operators.FilterOperatorMapper;
import eu.leads.processor.execute.operators.OperatorType;
import eu.leads.processor.plan.ExecutionPlan;
import eu.leads.processor.plan.ExecutionPlanNode;
import eu.leads.processor.plan.SelectExtractor;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.query.SQLQuery;
import eu.leads.processor.sql.PlanNode;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.Utilities;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.MapReduceTask;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/5/13
 * Time: 12:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class FilterOperatorTest {
    private static final int numOfTuples = 10;
    private static final String[] columnNames = {"url", "domain", "pagerank", "body", "sentiment"};
    private static final String[] columnType = {"string", "string", "double", "string", "double"};


    public static void main(String[] args) {
        InfinispanUtils.start();
//        Map<String, String> tables = InfinispanUtils.getOrCreatePersistentMap("tables");
        ConcurrentMap queries = InfinispanUtils.getOrCreatePersistentMap("queries");
        ObjectMapper mapper = new ObjectMapper();
        try {


            SQLQuery query = new SQLQuery("user", "location", "select url,domain from webpages where webpages.body like \'th\'", "SELECT");
            query.setId("q1");
            CCJSqlParserManager manager = new CCJSqlParserManager();
            Statement s = manager.parse(new StringReader("create table webpages(url varchar(100) PRIMARY KEY, domain varchar(100), body varchar(100),pagerank double,sentiment double)"));

            CreateTable tt = (CreateTable) s;
            List<String> tablestrings = (List<String>) tt.getTableOptionsStrings();
            TableInfo info = new TableInfo(tt.getTable(), tablestrings, tt.getColumnDefinitions());
            Statement st = manager.parse(new StringReader(query.getQueryText()));
            JsonNode root = mapper.readTree(mapper.writeValueAsString(st));
            SelectExtractor extractor = new SelectExtractor(root);
            QueryContext context = new QueryContext(query);
            context.addTable(info);
            query.setQueryContext(context);
            ExecutionPlan p = (ExecutionPlan) extractor.extractPlan("anoutput", context);
            ExecutionPlanNode filter = null;
            for (PlanNode node : p.getNodes()) {
                ExecutionPlanNode e = (ExecutionPlanNode) node;
                if (e.getType().equals(OperatorType.toString(OperatorType.FILTER))) {
                    filter = e;
                    break;
                }
            }
            queries.put(query.getId(), mapper.writeValueAsString(query));
            FilterOperator f = (FilterOperator) filter;

            Cache<String, String> inputMap = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap("testData");
            for (int i = 0; i < numOfTuples; i++) {
                Tuple t = Utilities.generateTuple(columnNames, columnType);
                inputMap.put("test:" + (Integer.toString(i)), t.asString());
            }
            System.out.println("intial");
            Utilities.printMap(inputMap);

            MapReduceTask<String, String, String, String> task = new MapReduceTask<String, String, String, String>(inputMap);
            Properties conf = new Properties();
            conf.setProperty("output", "testOutput");
            if(f.getTree() != null)
                conf.setProperty("tree", mapper.writeValueAsString(f.getTree()));
            conf.setProperty("queryId", context.getQueryId());
            task.mappedWith(new FilterOperatorMapper(conf)).reducedWith(new LeadsReducer<String, String>(conf));
            task.execute();

            System.out.println("filtered");
            Map<String, String> dataMap = InfinispanUtils.getOrCreatePersistentMap("data");
            Utilities.printMap(dataMap);
            System.out.println("goodbye");
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
