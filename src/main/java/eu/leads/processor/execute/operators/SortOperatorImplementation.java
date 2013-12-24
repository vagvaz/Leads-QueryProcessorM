package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.execute.TupleComparator;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.math.MathUtils;
import net.sf.jsqlparser.schema.Column;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.MapReduceTask;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 8:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class SortOperatorImplementation {
    private final String prefix;
    private final SortOperator sort;
    private final ConcurrentMap<String, String> inputMap;
    private QueryContext context;
//    ConcurrentMap<String, String> data;
//    Vector<Tuple> tuples;
//    QueryContext context;
final ArrayList<Boolean> arithmentic;

    public SortOperatorImplementation(String input, String output, SortOperator operator, QueryContext context) {
        this.prefix = output + ":";
        this.sort = operator;
        inputMap = InfinispanUtils.getOrCreatePersistentMap(input);
//        data = InfinispanUtils.getOrCreatePersistentMap("data");
//        System.out.println("input " + input + " output " + output);
//        tuples = new Vector<Tuple>();
//        this.context = context;
        this.context = context;
        arithmentic = new ArrayList<Boolean>(operator.getColumns().size());
        for (Column c : operator.getColumns()) {
            if (c.getTable() == null)
                arithmentic.add(true);
            else
                arithmentic.add(MathUtils.isArithmentic(context.getColumnType(c.getColumnName(), c.getTable().getName())));

        }
    }

    public void execute() {
//        for (Map.Entry<String, String> entry : inputMap.entrySet()) {
////            System.out.println("adding entry");
////            System.out.println(this.getClass().toString()+" proc tuple");
//            tuples.add(new Tuple(entry.getValue()));
//        }
//        //TODO
//        Comparator<Tuple> comparator = new TupleComparator(sort.getColumns(), sort.getAscending(), arithmentic);
//        Collections.sort(tuples, comparator);
//        int counter = 0;
//        for (Tuple t : tuples) {
//            data.put(prefix + Integer.toString(counter), t.asString());
//            counter++;
//        }
//    }
        MapReduceTask<String, String, String, String> task = new MapReduceTask<String, String, String, String>((Cache<String, String>) inputMap,false,false);
        Properties configuration = new Properties();
        ObjectMapper mapper = new ObjectMapper();
        try {
            configuration.setProperty("sortColumns", mapper.writeValueAsString(sort.getColumns()));
            configuration.setProperty("ascending", mapper.writeValueAsString(sort.getAscending()));
            configuration.setProperty("arithmetic", mapper.writeValueAsString(arithmentic));
            configuration.setProperty("parts", Integer.toString(3 * InfinispanUtils.getMembers().size()));
            configuration.setProperty("workload", Integer.toString(inputMap.size() / InfinispanUtils.getMembers().size()));
            configuration.setProperty("keysName",context.getQueryId()+".merge");
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        SortMapper map = new SortMapper(configuration);
        SortReducer reduce = new SortReducer(configuration);
        task.mappedWith(map).reducedWith(reduce);
        map = null;
        reduce = null;
        Map<String, String> caches = task.execute();
        SortMerger merger = new SortMerger(caches, prefix, new TupleComparator(sort.getColumns(), sort.getAscending(), arithmentic));
        merger.merge();
        caches.clear();
        InfinispanUtils.removeCache(context.getQueryId()+".merge");



    }
}
