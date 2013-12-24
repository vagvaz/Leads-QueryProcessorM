package eu.leads.processor;

import eu.leads.processor.execute.Tuple;
import eu.leads.processor.execute.operators.JoinMapper;
import eu.leads.processor.execute.operators.JoinOperator;
import eu.leads.processor.execute.operators.JoinReducer;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.Utilities;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.MapReduceTask;

import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/7/13
 * Time: 9:11 AM
 * To change this template use File | Settings | File Templates.
 */
public class JoinOperatorTest {
    private static final String[] columnNames = {"url", "anark", "pagerank", "body", "sentiment"};
    private static final String[] columnType = {"string", "string", "int", "string", "double"};
    private static final String[] columnNamesB = {"foo", "bbark", "dfs", "adf", "ddddd"};
    private static final String[] columnTypeB = {"string", "string", "int", "string", "double"};

    public static void main(String[] args) {
        Table l = new Table("", "webpages");
        Table r = new Table("", "project");
        Column lc = new Column(l, "url");
        Column lr = new Column(r, "foo");
        InfinispanUtils.start();
        int numOfTuples = 10;
        ConcurrentMap<String, String> data = InfinispanUtils.getOrCreatePersistentMap(l.getName() + ":");
        for (int i = 0; i < numOfTuples; i++) {
            Tuple t = Utilities.generateTuple(columnNames, columnType);
            data.put(l.getName() + ":" + (Integer.toString(i)), t.asString());
        }

        data = InfinispanUtils.getOrCreatePersistentMap(r.getName() + ":");
        for (int i = 0; i < numOfTuples; i++) {
            Tuple t = Utilities.generateTuple(columnNamesB, columnTypeB);
            data.put(r.getName() + ":" + (Integer.toString(i)), t.asString());
        }
        JoinOperator join = new JoinOperator("test", "theoutput", l, r, lc, lr);
        Properties conf = new Properties();
        conf.setProperty("left", join.getLeft().getName() + ":");
        conf.setProperty("right", join.getRight().getName() + ":");
        conf.setProperty(join.getRight().getName() + ":", join.getRightColumn().getColumnName());
        conf.setProperty(join.getLeft().getName() + ":", join.getLeftColumn().getColumnName());
        conf.setProperty("output", join.getName());
        ConcurrentMap<String, String> inputMap = InfinispanUtils.getOrCreatePersistentMap(join.getName() + ".input");
        inputMap.put(join.getLeft().getName() + ":", join.getLeft().getName() + ":");
        inputMap.put(join.getRight().getName() + ":", join.getRight().getName() + ":");
        MapReduceTask<String, String, String, String> task = new MapReduceTask<String, String, String, String>((Cache<String, String>) inputMap);
        task.mappedWith(new JoinMapper(conf)).reducedWith(new JoinReducer(conf));
        task.execute();
        ConcurrentMap<String, String> out = InfinispanUtils.getOrCreatePersistentMap(join.getName() + ":");
        Utilities.printMap(out);
        System.out.println("bye bye");
        InfinispanUtils.stop();

//        output
        //and create a map for keys
    }
}
