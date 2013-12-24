package eu.leads.processor.execute.operators;

import eu.leads.processor.execute.Tuple;
import eu.leads.processor.execute.TupleComparator;
import eu.leads.processor.utils.InfinispanUtils;

import java.util.Map;
import java.util.Vector;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 12/3/13
 * Time: 11:03 AM
 * To change this template use File | Settings | File Templates.
 */
public class SortMerger {

//    private Map<String, String> input;
//    private String output;
    private final String prefix;
    private final Map<String, String> outputMap;
    private Vector<Integer> counters;
    private Vector<Tuple> values;
    private Vector<String> keys;
    private Vector<Map<String, String>> caches;
    private final TupleComparator comparator;
    private Vector<String> cacheNames;
    public SortMerger(Map<String, String> inputMap, String output, TupleComparator comp) {

        prefix = output;
//        this.output = output;
//        input = inputMap;
        outputMap = InfinispanUtils.getOrCreatePersistentMap(prefix);
        counters = new Vector<Integer>(inputMap.keySet().size());
        values = new Vector<Tuple>(inputMap.keySet().size());
        caches = new Vector<Map<String, String>>();
        cacheNames = new Vector<String>(inputMap.size());
        keys = new Vector<String>(inputMap.size());
        comparator = comp;
        for (Map.Entry<String, String> entry : inputMap.entrySet()) {
            counters.add(0);
            keys.add(entry.getKey());
            caches.add(InfinispanUtils.getOrCreatePersistentMap(entry.getValue()));
            Tuple t = getCurrentValue(keys.size() - 1);
            values.add(t);
            cacheNames.add(entry.getValue());

        }
    }

    private Tuple getCurrentValue(int cacheIndex) {
        String key = keys.get(cacheIndex);
        Integer counter = counters.get(cacheIndex);
        String tmp = caches.get(cacheIndex).get(key + ":" + counter.toString());
        return new Tuple(tmp);
    }

    private Tuple getNextValue(int cacheIndex) {
        String key = keys.get(cacheIndex);
        Integer counter = counters.get(cacheIndex);
        counter = counter + 1;
        if (counter >= caches.get(cacheIndex).size()) {
            counters.remove(cacheIndex);
            caches.remove(cacheIndex);
            InfinispanUtils.removeCache(cacheNames.elementAt(cacheIndex));
            cacheNames.removeElementAt(cacheIndex);
            keys.remove(cacheIndex);
            values.remove(cacheIndex);
            return null;
        }
        counters.set(cacheIndex, counter);
        String tmp = caches.get(cacheIndex).get(key + ":" + counter.toString());
        return new Tuple(tmp);
    }

    public void merge() {
        Tuple nextValue = null;
        Tuple t = null;
        while (caches.size() > 0) {
            int minIndex = findMinIndex(values);

            t = values.get(minIndex);
            outputMap.put(prefix + outputMap.size(), t.asString());

            nextValue = getNextValue(minIndex);
            if (nextValue != null)
                values.set(minIndex, nextValue);
        }
        counters.clear();
        counters = null;
        for(String cache : keys){
            InfinispanUtils.removeCache(cache);
        }
        keys.clear();
        keys = null;
        values.clear();
        values = null;
        cacheNames.clear();
        cacheNames = null;
        for (Map<String, String> map : caches) {
            map.clear();
        }
        caches.clear();
        caches = null;
    }

    private int findMinIndex(Vector<Tuple> values) {
        int result = 0;
        Tuple curMin = values.get(0);
        for (int i = 1; i < values.size(); i++) {
            int cmp = comparator.compare(curMin, values.get(i));
            if (cmp > 0) {
                curMin = values.get(i);
                result = i;
            }

        }
        return result;

    }
}
