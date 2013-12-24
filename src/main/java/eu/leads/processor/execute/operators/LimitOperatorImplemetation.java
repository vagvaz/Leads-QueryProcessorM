package eu.leads.processor.execute.operators;

import eu.leads.processor.execute.Tuple;
import eu.leads.processor.utils.InfinispanUtils;
import net.sf.jsqlparser.statement.select.Limit;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

//import pagerank.graph.LeadsPrGraph;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 8:40 AM
 * To change this template use File | Settings | File Templates.
 */
//Limit operator just reads the input Cache and keeps only the first limit it encounters
// if the input is sorted then it reads in asennding order the keys -> input_cache_name:0,input_cache_name:1 etc
public class LimitOperatorImplemetation {
    final String prefix;
    final Limit limit;
    final ConcurrentMap<String, String> inputMap;
    final ConcurrentMap<String, String> data;
    boolean sorted = false;
    final String input;

    public LimitOperatorImplemetation(String input, String output, Limit limit, boolean isSorted) {
        this.prefix = output + ":";
        this.limit = limit;
        inputMap = InfinispanUtils.getOrCreatePersistentMap(input);
        data = InfinispanUtils.getOrCreatePersistentMap(prefix);
        sorted = isSorted;
        this.input = input;
    }

    public void execute() {
        int counter = 0;
        if (sorted) {
            int sz = inputMap.size();
            for (counter = 0; counter < limit.getRowCount() && counter < sz; counter++) {
                String tupleValue = inputMap.get(input + counter);
                Tuple t = new Tuple(tupleValue);
                handlePagerank(t);
                data.put(prefix + Integer.toString(counter), t.asString());
            }
        } else {
            for (Map.Entry<String, String> entry : inputMap.entrySet()) {
                if (counter >= limit.getRowCount())
                    break;
                String tupleId = entry.getKey().substring(entry.getKey().indexOf(":") + 1);
                Tuple t = new Tuple(entry.getValue());
                handlePagerank(t);
                data.put(prefix + tupleId, t.asString());
                counter++;
            }
        }
    }

    private void handlePagerank(Tuple t) {
        if (t.hasField("pagerank")) {
            if (!t.hasField("url"))
                return;
            String pagerankStr = t.getAttribute("pagerank");
//            Double d = Double.parseDouble(pagerankStr);
//            if (d < 0.0) {

//                try {
////                    d = LeadsPrGraph.getPageDistr(t.getAttribute("url"));
//                    d = (double) LeadsPrGraph.getPageVisitCount(t.getAttribute("url"));
//                    System.out.println("vs cnt " + LeadsPrGraph.getTotalVisitCount());
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                t.setAttribute("pagerank", d.toString());
//            }
            }
        }
    }
