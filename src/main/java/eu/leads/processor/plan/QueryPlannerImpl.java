package eu.leads.processor.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.Module;
import eu.leads.processor.query.Query;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.query.SQLQuery;
import eu.leads.processor.sql.Plan;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.StdOutputWriter;
import eu.leads.processor.utils.StringConstants;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/9/13
 * Time: 8:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class QueryPlannerImpl extends Module implements QueryPlanner {
    private ConcurrentMap queriesCache;
    private Queue<Message> incoming;
    private ObjectMapper mapper;
    private final Object mutex = new Object();


    public QueryPlannerImpl(String url, String name) throws Exception {
        super(url, name);
        com.subscribeToQueue(StringConstants.PLANNERQUEUE);
        com.createQueuePublisher(StringConstants.DEPLOYERQUEUE);
        com.setTopicMessageListener(this);
        com.setQueueMessageListener(this);
        mapper = new ObjectMapper();
        incoming = new LinkedList<Message>();
        queriesCache = InfinispanUtils.getOrCreatePersistentMap(StringConstants.QUERIESCACHE);
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
                    if (type.equals("sqlTreePlan")) {
                        String queryType = message.getStringProperty("queryType");
                        SQLQuery query = new SQLQuery(message.getStringProperty("user"), message.getStringProperty("location"), message.getText(), queryType);
                        query.setId(message.getStringProperty("id"));
                        StdOutputWriter.getInstance().println("QueryPlanner is ready to produce plans for " + query.getId());
                        //validate query and extract the basic plan
                        if (validateQuery(query)) {
                            //Generate plans
                            //Right now do nothing
                            SortedMap<Double, Plan> plans = generatePlans(query);
                            //Interface with Scheduler
                            evaluatePlansFromScheduler();
                            //Choose plan from the evaluated
                            Plan selected = choosePlan(plans);
                            //Send selected plan to deployer
                            sendPlanToDeployer(selected, query);
                        } else {
                            //TODO Error reporting
                        }
                    } else {

                    }
                }
            }
        }
    }

    private boolean validateQuery(SQLQuery query) {
        boolean result = true;
        QueryContext context = new QueryContext(query);
        query.setQueryContext(context);
        //Read Query
        CCJSqlParserManager manager = new CCJSqlParserManager();
        Statement st;
        String jsonStatement;
        JsonNode root = null;
        try {
            st = manager.parse(new StringReader(query.getQueryText()));
            jsonStatement = mapper.writeValueAsString(st);
            root = mapper.readTree(jsonStatement);
        } catch (JSQLParserException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (JsonProcessingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Extract basic plan and set it to query.
        BasicPlannerExtractor extractor = null;
        try {
            extractor = ExtractorFactory.getBasicExtractor(StatementType.getType(query.getSqlType()), root);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        Plan p = extractor.extractPlan(context.getQueryId() + "output", context);

        query.setPlan(p);
        return result;
    }

    private void sendPlanToDeployer(Plan selected, Query query) {
        TextMessage message = new ActiveMQTextMessage();
        try {
            //update query to hold the selected plan
            query.setPlan(selected);
            //Store query to KVS

            queriesCache = InfinispanUtils.getOrCreatePersistentMap(StringConstants.QUERIESCACHE);
            queriesCache.put(query.getId(), mapper.writeValueAsString(query));
            message.setText(mapper.writeValueAsString(query.getPlan()));
            message.setStringProperty("context", mapper.writeValueAsString(query.getContext()));
            message.setStringProperty("type", "execute");
        } catch (JMSException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (JsonProcessingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        try {
            com.publishToQueue(message, StringConstants.DEPLOYERQUEUE);
        } catch (JMSException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    private SortedMap<Double, Plan> evaluatePlansFromScheduler() {
        //TODO
        SortedMap<Double, Plan> result = new TreeMap<Double, Plan>();
        return result;
    }

    @Override
    public void onMessage(Message message) {
//        System.err.println(this.getClass().toString() + " received msg");
        String messageType = "";

        synchronized (mutex) {
            incoming.add(message);
            mutex.notify();
        }
    }

    @Override
    public SortedMap generatePlans(Query q) {
        SortedMap<Double, Plan> result = new TreeMap<Double, Plan>();
        result.put(1.0, q.getPlan());
        return result;
    }


    @Override
    public Plan choosePlan(SortedMap<Double, Plan> plans) {
        Plan result = null;
        Double headKey = plans.firstKey();
        result = plans.get(headKey);
        return result;
    }

    @Override
    protected void triggerShutdown() {
        synchronized (mutex) {
            mutex.notify();
        }
        super.triggerShutdown();
    }

}
