package eu.leads.processor.execute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import eu.leads.processor.Module;
import eu.leads.processor.execute.operators.*;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.StdOutputWriter;
import eu.leads.processor.utils.StringConstants;
import net.sf.jsqlparser.schema.Column;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;

import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/30/13
 * Time: 3:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class NodeQueryExecutor extends Module {
    ConcurrentMap<String, String> queries;
    Queue<Message> incoming;
    private ObjectMapper mapper;
    private String executorQueue = StringConstants.NODEEXECUTORQUEUE;
    private final Object mutex = new Object();

    public NodeQueryExecutor(String url, String name) throws Exception {
        super(url, name);
        incoming = new LinkedList<Message>();
        queries = InfinispanUtils.getOrCreatePersistentMap(StringConstants.QUERIESCACHE);
        com.subscribeToQueue(executorQueue);
        com.createQueuePublisher(StringConstants.DEPLOYERQUEUE + ".monitor");
        com.setQueueMessageListener(this);
        mapper = new ObjectMapper();
    }

    @Override
    protected void run() throws Exception {
        while (isRunning()) {
            Queue<Message> unprocessed = null;
            synchronized (mutex) {
                if (incoming.size() > 0) {
                    unprocessed = incoming;
                    incoming = new LinkedList<Message>();
                } else {
                    mutex.wait();
                }
            }

            if (unprocessed != null) {
                while (!unprocessed.isEmpty()) {
                    TextMessage message = (TextMessage) unprocessed.poll();
                    String type = message.getStringProperty("type");
                    //Get The QueryDployer which node query executor must reply.
                    String replyTo = message.getStringProperty("replyTo");
                    if (Strings.isNullOrEmpty(replyTo)) {
                        replyTo = StringConstants.DEPLOYERQUEUE + ".monitor";
                    }

                    String operatorName = "";
                    //execute an operator.
                    if (type.equals("executeOperator")) {
                        String input = message.getStringProperty("input");
                        input += ":";
                        String operator = message.getText();
                        JsonNode root = mapper.readTree(operator);
                        operatorName = root.path("name").asText();
                        String queryId = message.getStringProperty("queryId");
                        String query = (String) queries.get(queryId);
                        JsonNode queryRoot = mapper.readTree(query);
                        QueryContext context = mapper.readValue(queryRoot.path("context").toString(), QueryContext.class);
                        Cache<String, String> inputMap;
                        Properties conf = null;
                        MapReduceTask<String, String, String, String> task;
                        Mapper<String, String, String, String> map;
                        Reducer<String, String> reduce;
                        //Switch for different operator types
                        switch (OperatorType.fromString(root.path("operatorType").asText())) {
                            case READ:
                                //read has not implemetation
                                break;
                            case JOIN:
                                //join is implemeted with a mapreduce task      select domainName,sum(pagerank) from webpages where pagerank > 0 group by domainName having sum(pagerank) > 1 order by sum(pagerank);

                                JoinOperator join = mapper.readValue(operator, JoinOperator.class);
                                StdOutputWriter.getInstance().println("Executing " + join.toString());
                                conf = new Properties();
                                conf.setProperty("left", join.getLeft().getName() + ":");
                                conf.setProperty("right", join.getRight().getName() + ":");
                                conf.setProperty(join.getRight().getName() + ":", join.getRightColumn().getColumnName());
                                conf.setProperty(join.getLeft().getName() + ":", join.getLeftColumn().getColumnName());
                                conf.setProperty("output", join.getName());

                                ConcurrentMap<String, String> jinputMap = InfinispanUtils.getOrCreatePersistentMap(join.getName() + ".input");
                                jinputMap.put(join.getLeft().getName() + ":", join.getLeft().getName() + ":");
                                jinputMap.put(join.getRight().getName() + ":", join.getRight().getName() + ":");
                                conf.setProperty("workload", Integer.toString(jinputMap.size() / InfinispanUtils.getMembers().size()));
                                task = new MapReduceTask<String, String, String, String>((Cache<String, String>) jinputMap);
                                task.timeout(1, TimeUnit.HOURS);
                                map = new JoinMapper(conf);
                                reduce = new JoinReducer(conf);
                                task.mappedWith(map).reducedWith(reduce);
                                task.execute();
                                InfinispanUtils.removeCache(join.getName() + ".input");
                                break;
                            case FILTER:
                                //Filter is implemeted with a mapreduce task (use only mappers)
                                FilterOperator f = mapper.readValue(operator, FilterOperator.class);
                                StdOutputWriter.getInstance().println("Executing " + f.toString());
                                inputMap = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap(input);
                                task = new MapReduceTask<String, String, String, String>(inputMap, false, false);
                                task.timeout(1, TimeUnit.HOURS);
                                conf = new Properties();
                                conf.setProperty("output", f.getName());
                                conf.setProperty("tree", mapper.writeValueAsString(f.getTree()));
                                conf.setProperty("queryId", queryId);
                                conf.setProperty("workload", Integer.toString(inputMap.size() / InfinispanUtils.getMembers().size()));
                                map = new FilterOperatorMapper(conf);
                                reduce = new LeadsReducer<String, String>(conf);
                                task.mappedWith(map).reducedWith(reduce);
                                Map<String, String> result = task.execute();
                                break;
                            case OUTPUT:
                                break;
                            case GROUPBY:
                                //Group by operator is implemeted with a mapreduce task
                                JsonNode json = mapper.readTree(operator);
                                GroupByJsonDelegate converter = new GroupByJsonDelegate();
                                GroupByOperator group = converter.convert(json);
                                StdOutputWriter.getInstance().println("Executing " + group.toString());
                                inputMap = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap(input);
                                task = new MapReduceTask<String, String, String, String>(inputMap,false,false);
                                task.timeout(1, TimeUnit.HOURS);
                                conf = new Properties();
                                conf.setProperty("output", group.getName());
                                conf.setProperty("functions", mapper.writeValueAsString(group.getFunctions()));
                                conf.setProperty("context", mapper.writeValueAsString(context));
                                conf.setProperty("workload", Integer.toString(inputMap.size() / InfinispanUtils.getMembers().size()));
                                StringBuilder builder = new StringBuilder();
                                for (Column c : group.getColumns()) {
                                    builder.append(c.getColumnName() + ",");
                                }
                                String columns = "";
                                if (builder.length() > 0)
                                    columns = builder.toString().substring(0, builder.length() - 1);
                                conf.setProperty("columns", columns);
                                task.mappedWith(new GroupByMapper(conf)).reducedWith(new GroupByReducer(conf));
                                task.execute();
                                break;
                            case SORT:
                                //Sort implemetation is with one mapreduce task and then we combine results
                                SortOperator sort = mapper.readValue(operator, SortOperator.class);
                                StdOutputWriter.getInstance().println("Executing " + sort.toString());
                                SortOperatorImplementation sortImpl = new SortOperatorImplementation(input, sort.getName(), sort, context);
                                sortImpl.execute();
                                break;
                            case LIMIT:
                                LimitOperator limit = mapper.readValue(operator, LimitOperator.class);
                                StdOutputWriter.getInstance().println("Executing " + limit.toString());
                                boolean isSorted = false;
                                if (queryRoot.has("isSorted")) {
                                    isSorted = true;
                                }
                                LimitOperatorImplemetation op = new LimitOperatorImplemetation(input, limit.getName(), limit.getLimit(), isSorted);
                                op.execute();
                                break;
                            case RENAME:
                                break;
                            case PROJECT:
                                ProjectOperator project = mapper.readValue(operator, ProjectOperator.class);
                                StdOutputWriter.getInstance().println("Executing " + project.toString());
                                inputMap = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap(input);
                                task = new MapReduceTask<String, String, String, String>(inputMap, false, false);
                                task.timeout(1, TimeUnit.HOURS);
                                conf = new Properties();
                                conf.setProperty("output", project.getName());
                                conf.setProperty("columns", mapper.writeValueAsString(project.getColumns()));
                                conf.setProperty("data", "data");
                                conf.setProperty("workload", Integer.toString(inputMap.size() / InfinispanUtils.getMembers().size()));
                                map = new ProjectMapper(conf);
                                reduce = new LeadsReducer<String, String>(conf);
                                task.mappedWith(map).reducedWith(reduce);
                                task.execute();
                                break;
                            default:
                                break;
                        }
                        StdOutputWriter.getInstance().println("Execution is completed");
                        map = null;
                        reduce = null;
                        //force running garbage collector in order to stop reporting progress of the mapper and reducer used for the execution of the operator.
                        TextMessage completeMessage = new ActiveMQTextMessage();
                        completeMessage.setText(operatorName);
                        completeMessage.setStringProperty("queryId", queryId);
                        completeMessage.setStringProperty("type", "completion");
                        com.publishToQueue(completeMessage, replyTo);
                        completeMessage = null;
                        task = null;
                        if(conf != null)
                        conf.clear();
                        conf = null;
                        message = null;
                        Runtime.getRuntime().gc();                      //Inform deploery module for operator completion



                    }
                }
            }
        }
    }

    @Override
    public void onMessage(Message message) {
//        System.err.println(this.getClass().toString() + " received msg");
        synchronized (mutex) {
            incoming.add(message);
            mutex.notify();
        }
    }

    @Override
    protected void triggerShutdown() {
        synchronized (mutex) {
            mutex.notify();
        }
        super.triggerShutdown();
    }
}
