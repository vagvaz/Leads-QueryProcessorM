package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.LeadsReducer;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.execute.TupleComparator;
import eu.leads.processor.utils.InfinispanUtils;
import net.sf.jsqlparser.schema.Column;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 12/3/13
 * Time: 10:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class SortReducer extends LeadsReducer<String, String> {
    transient private List<Column> sortColumns;
    private List<Boolean> isAscending;
    private List<Boolean> arithmetic;
    private String output;
    ConcurrentMap<String, String> out;

    public SortReducer(Properties configuration) {
        super(configuration);
    }

    @Override
    public void initialize() {
        isInitialized = true;
        super.initialize();
        String columns = conf.getProperty("sortColumns");
        String ascending = conf.getProperty("ascending");
        String arithm = conf.getProperty("arithmetic");
        ObjectMapper mapper = new ObjectMapper();
        try {
            sortColumns = mapper.readValue(columns, new TypeReference<List<Column>>() {
            });
            isAscending = mapper.readValue(ascending, new TypeReference<List<Boolean>>() {
            });
            arithmetic = mapper.readValue(arithm, new TypeReference<List<Boolean>>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        output = conf.getProperty("keysName");

    }

    @Override
    public String reduce(String key, Iterator<String> iterator) {
        if (!isInitialized)
            initialize();

        out = InfinispanUtils.getOrCreatePersistentMap(output + key);
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Comparator<Tuple> comparator = new TupleComparator(sortColumns, isAscending, arithmetic);
        while (iterator.hasNext()) {
            String tmp = iterator.next();
            tuples.add(new Tuple(tmp));
            progress();
        }
        Collections.sort(tuples, comparator);
        int counter = 0;
        for (Tuple t : tuples) {
            out.put(key + ":" + counter, t.asString());
            counter++;
        }
        tuples.clear();
        tuples = null;
        comparator = null;
        return output + key;
    }
}
