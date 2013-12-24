package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.LeadsReducer;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.SQLUtils;
import eu.leads.processor.utils.math.MathUtils;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import org.infinispan.Cache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import static java.lang.System.getProperties;

//import pagerank.graph.LeadsPrGraph;

//import pagerank.graph.LeadsPrGraph;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 9:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class GroupByReducer extends LeadsReducer<String, String> {

    String prefix;
    Cache<String, String> data;
    transient List<Function> functions;
    final HashMap<String, Number> functionValues;
    final HashMap<String, String> columnTypes;
    transient QueryContext context;

    public GroupByReducer(Properties configuration) {
        super(configuration);
        functions = new ArrayList<Function>();
        functionValues = new HashMap<String, Number>();
        columnTypes = new HashMap<String, String>();


    }

    @Override
    public void initialize() {

        isInitialized = true;
        super.initialize();
        prefix = conf.getProperty("output") + ":";
        data = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap(prefix);
        ObjectMapper mapper = new ObjectMapper();
        String funcs = conf.getProperty("functions");
//        try {
//            Thread.sleep(4000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        String contextString = conf.getProperty("context");
        if (contextString != null) {
            try {
                context = mapper.readValue(contextString, QueryContext.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (funcs != null) {
            try {
                functions = new ArrayList<Function>();
                JsonNode root = mapper.readTree(funcs);
                Iterator<JsonNode> iterator = root.elements();
                while (iterator.hasNext()) {
                    JsonNode node = iterator.next();
                    Function f = SQLUtils.extractFunction(node);
                    functions.add(f);
                }
                for (Function func : functions) {
                    ExpressionList elist = func.getParameters();
                    if (elist != null) {
                        for (Expression e : elist.getExpressions()) {
                            if (e instanceof Column) {
                                Column c = (Column) e;
                                columnTypes.put(func.toString(), context.getColumnType(c.getColumnName(), c.getTable().getName()));
                            }

                        }
                    } else {

                        columnTypes.put(func.getName() + "(*)", "string");
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
//        String hostname = "";
//        try {
//            hostname = InetAddress.getLocalHost().getHostName();
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
        RandomAccessFile raf = null;
        try {
            String filename = getProperties().getProperty("java.io.tmpdir") + "/queryProcessor." + InfinispanUtils.getMemberName();
            File f = new File(filename);
            long fileLength = f.length();
            raf = new RandomAccessFile(filename, "rw");
            raf.seek(fileLength);

            raf.writeBytes("Running " + InfinispanUtils.getMemberName() + ": " + this.getClass().getCanonicalName() + "\n");
            raf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public String reduce(String key, Iterator<String> iterator) {
        if (!isInitialized) initialize();
        Tuple t = null;
        progress();
        while (iterator.hasNext()) {

            t = new Tuple(iterator.next());
            handlePagerank(t);
            for (Function f : functions) {
                if (f.getName().equalsIgnoreCase("COUNT")) {
                    updateCount(f);
                } else if (f.getName().equalsIgnoreCase("SUM")) {
                    updateSum(f, t);
                } else if (f.getName().equalsIgnoreCase("AVG")) {
                    updateAverage(f, t);

                } else if (f.getName().equalsIgnoreCase("MAX")) {
                    updateMax(f, t);

                } else if (f.getName().equalsIgnoreCase("MIN")) {
                    updateMin(f, t);

                }
            }
        }
        for (Function f : functions) {
            if (!f.getName().equalsIgnoreCase("AVG")) {
                t.setAttribute(f.toString(), functionValues.get(f.toString()).toString());
            } else {
                long count = functionValues.get(f.toString() + ".count").longValue();
                double sum = functionValues.get(f.toString() + ".sum").doubleValue();
                t.setAttribute(f.toString(), Double.toString((double) sum / (double) count));

            }
        }
//        System.err.println(this.getClass().toString()+" proc tuple ");

        data.put(prefix + key, t.asString());

//        System.err.println(this.getClass().toString()+" proc tuple " + prefix+key + " "+ data.size());
        functionValues.clear();
        return "";
    }

    private void handlePagerank(Tuple t) {
        if (t.hasField("pagerank")) {
            if (!t.hasField("url"))
                return;
            String pagerankStr = t.getAttribute("pagerank");
//            Double d = Double.parseDouble(pagerankStr);
//            if (d < 0.0) {
//
//                try {
////                    d = LeadsPrGraph.getPageDistr(t.getAttribute("url"));
//                    d = (double) LeadsPrGraph.getPageVisitCount(t.getAttribute("url"));
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                t.setAttribute("pagerank", d.toString());
//            }
        }
    }

    private void updateSum(Function f, Tuple t) {
        if (functionValues.containsKey(f.toString())) {
            Number l = functionValues.get(f.toString());
            String opType = MathUtils.handleType(columnTypes.get(f.toString()));
            if (opType.equals("double")) {
                Double d = Double.parseDouble(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                d += l.doubleValue();
                l = d;
            } else {
                Long ll = Long.parseLong(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                ll += l.longValue();
                l = ll;
            }
            functionValues.put(f.toString(), l);
        } else {
            Number l;
            String opType = MathUtils.handleType(columnTypes.get(f.toString()));
            if (opType.equals("double")) {
                Double d = Double.parseDouble(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                l = d;
            } else {
                Long ll = Long.parseLong(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                l = ll;
            }
            functionValues.put(f.toString(), l);
        }
    }

    private void updateAverage(Function f, Tuple t) {
        if (functionValues.containsKey(f.toString() + ".sum")) {
            Number l = functionValues.get(f.toString() + ".sum");
            String opType = MathUtils.handleType(columnTypes.get(f.toString()));
            if (opType.equals("double")) {
                Double d = Double.parseDouble(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                d += l.doubleValue();
                l = d;
            } else {
                Long ll = Long.parseLong(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                ll += l.longValue();
                l = ll;
            }
            functionValues.put(f.toString() + ".sum", l);
        } else {
            Number l;
            String opType = MathUtils.handleType(columnTypes.get(f.toString()));
            if (opType.equals("double")) {
                Double d = Double.parseDouble(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                l = d;
            } else {
                Long ll = Long.parseLong(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                l = ll;
            }
            functionValues.put(f.toString() + ".sum", l);
        }

        if (functionValues.containsKey(f.toString() + ".count")) {
            Long l = (Long) functionValues.get(f.toString() + ".count");
            l++;
            functionValues.put(f.toString() + ".count", l);
        } else {
            Long l = 1L;
            functionValues.put(f.toString() + ".count", l);
        }
    }

    private void updateMin(Function f, Tuple t) {
        if (functionValues.containsKey(f.toString())) {
            Number l = functionValues.get(f.toString());
            String opType = MathUtils.handleType(columnTypes.get(f.toString()));
            if (opType.equals("double")) {
                Double d = Double.parseDouble(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                l = Math.min(l.doubleValue(), d);
            } else {
                Long ll = Long.parseLong(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                l = Math.min(l.longValue(), ll);

            }
            functionValues.put(f.toString(), l);
        } else {
            Number l;
            String opType = MathUtils.handleType(columnTypes.get(f.toString()));
            if (opType.equals("double")) {
                l = Double.parseDouble(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
            } else {
                l = Long.parseLong(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
            }

            functionValues.put(f.toString(), l);
        }

    }

    private void updateMax(Function f, Tuple t) {
        if (functionValues.containsKey(f.toString())) {
            Number l = functionValues.get(f.toString());
            String opType = MathUtils.handleType(columnTypes.get(f.toString()));
            if (opType.equals("double")) {
                Double d = Double.parseDouble(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                l = Math.max(l.doubleValue(), d);
            } else {
                Long ll = Long.parseLong(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
                l = Math.max(l.longValue(), ll);

            }
            functionValues.put(f.toString(), l);
        } else {
            Number l;
            String opType = MathUtils.handleType(columnTypes.get(f.toString()));
            if (opType.equals("double")) {
                l = Double.parseDouble(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
            } else {
                l = Long.parseLong(t.getAttribute(((Column) f.getParameters().getExpressions().get(0)).getColumnName()));
            }

            functionValues.put(f.toString(), l);
        }
    }


    private void updateCount(Function f) {
        if (functionValues.containsKey(f.toString())) {
            Long l = (Long) functionValues.get(f.toString());
            l++;
            functionValues.put(f.toString(), l);
        } else {
            Long l = 1L;
            functionValues.put(f.toString(), l);
        }
    }
}
