package eu.leads.processor.ui;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.leads.processor.Module;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.query.Query;
import eu.leads.processor.query.SQLQuery;
import eu.leads.processor.query.WorkflowQuery;
import eu.leads.processor.sql.QueryVisitor;
import eu.leads.processor.utils.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import org.apache.activemq.command.ActiveMQTextMessage;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/8/13
 * Time: 12:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class UserInterfaceManager extends Module {

    private ConcurrentMap queriesCache;
    private CCJSqlParserManager manager;
    private ObjectMapper mapper;
    private static final CommandLineUtil log = new CommandLineUtil();//Logger.getLogger(QueryProcessor.class.getName());
    private final Object mutex = new Object();
    private LinkedList<Message> incoming;

    public UserInterfaceManager(String url, String name) throws Exception {
        super(url, name);
        com.subscribeToQueue(StringConstants.UIMANAGERQUEUE);
        com.createQueuePublisher(StringConstants.PLANNERQUEUE);
        com.setTopicMessageListener(this);
        com.setQueueMessageListener(this);
        manager = new CCJSqlParserManager();
        mapper = new ObjectMapper();
        queriesCache = InfinispanUtils.getOrCreatePersistentMap(StringConstants.QUERIESCACHE);
        incoming = new LinkedList<Message>();
    }


    public String generateNewQueryId(Query query) {
        UUID id = UUID.randomUUID();
        while (queriesCache.containsKey(query.getUser() + "." + id)) {
            id = UUID.randomUUID();
        }
        return id.toString();
    }

    public void processQuery(SQLQuery query) throws JsonProcessingException {
        query.setId(query.getUser() + generateNewQueryId(query));
        Statement st = null;
        try {
            st = manager.parse(new StringReader(query.getQueryText()));
        } catch (JSQLParserException e) {
            log.error(e.toString());
        }
        try {
            Map<String, String> queries = InfinispanUtils.getOrCreatePersistentMap("queries");
            queries.put(query.getId(), mapper.writeValueAsString(query));
            sendAck(query);
        } catch (JMSException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        try {
            sendQueryToPlanner(query, getQueryType(st), query.getQueryText());
        } catch (JMSException e) {
            e.printStackTrace();
        }


    }

    private String getQueryType(Statement st) {
        if (st != null) {
            QueryVisitor visitor = new QueryVisitor();
            st.accept(visitor);
            return visitor.getStatementType();
        }
        return "";
    }

    private void sendQueryToPlanner(Query query, String type, String treePlan) throws JMSException {
        TextMessage message;
        message = new ActiveMQTextMessage();
        message.setText(treePlan);
        message.setStringProperty("user", query.getUser());
        message.setStringProperty("location", query.getLocation());
        message.setStringProperty("id", query.getId());
        message.setStringProperty("type", "sqlTreePlan");
        message.setStringProperty("queryType", type);
        com.publishToQueue(message, "PLANNERQUEUE");
    }

    private void sendAck(Query query) throws JMSException {
        TextMessage reply = new ActiveMQTextMessage();
        reply.setText(query.getId());
        reply.setStringProperty("type", "ack");
        reply.setText(query.getId());
        reply.setStringProperty("sqlText", ((SQLQuery) query).getQueryText());
//        System.out.println("ack..." + query.getLocation());
        com.publishToDestination(reply, query.getLocation());
    }

    public void processQuery() {
        throw new RuntimeException("Unsupported Query");
    }


    Set<Tuple> fecthResults(Query query) {
        Query stored = (Query) queriesCache.get(query.getId());
        Set<Tuple> result = new HashSet<Tuple>();
        if (stored.isCompleted()) {
//            Set<String> resultKeys = InfinispanUtils.getOrCreateSet(query.getId() + ".results");
        }
        return result;
    }

    @Override
    protected void run() throws Exception {

        while (isRunning()) {
            LinkedList<Message> toprocess = null;
            synchronized (mutex) {
                if (incoming.size() > 0) {
                    toprocess = incoming;
                    incoming = new LinkedList<Message>();
                } else {
                    mutex.wait();
                }
            }
            if (toprocess != null) {
                while (toprocess.size() > 0) {
                    Message message = toprocess.poll();
                    try {
                        if (message.getStringProperty("type").equals("SQLQueryMessage")) {
//                log.info("SQLQUEry message");
                            StdOutputWriter.getInstance().println("UserInterfaceManager received a SQLQuery");
                            TextMessage msg = (TextMessage) message;
                            SQLQuery newQuery = new SQLQuery(message.getStringProperty("user"), message.getJMSReplyTo().toString(), msg.getText(), SQLUtils.getSQLType(msg.getText()));
                            processQuery(newQuery);

                        } else if (message.getStringProperty("type").equals("isCompleted")) {

                            TextMessage msg = (TextMessage) message;
                            String id = msg.getText();
                            StdOutputWriter.getInstance().println("UserInterfaceManager received a request for completion of query " + id);
                            replyCompleted(id);

                        } else if (message.getStringProperty("type").equals("getResults")) {
                            TextMessage msg = (TextMessage) message;
                            String id = msg.getText();
                            StdOutputWriter.getInstance().println("UserInterfaceManager received a request for the results of query " + id);
                            replyGetResults(id);
                        } else if (message.getStringProperty("type").equals("queryCompletion")) {
                            TextMessage msg = (TextMessage) message;
                            replyCompleted(msg.getText());
                            StdOutputWriter.getInstance().println("UserInterfaceManager was informed that query " + msg.getText() + " has been completed");
                        }
                    } catch (JMSException e) {
                        e.printStackTrace();
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }
            }

        }
    }

    @Override
    public void onMessage(Message message) {
//        System.err.println(this.getClass().toString() + " received msg");
        synchronized (mutex) {
            if (message == null)
                return;
            incoming.add(message);
            mutex.notify();
        }
    }

    private void replyCompleted(String id) {
        TextMessage msg = new ActiveMQTextMessage();
        String query = (String) queriesCache.get(id);
        try {
            msg.setStringProperty("type", "completion");

            msg.setText(id);
        } catch (JMSException e) {
            e.printStackTrace();
        }
        try {
            //read json object for query
            JsonNode root = mapper.readTree(query);
            ObjectNode node = (ObjectNode) root;
            //check if query is  completed
            if (node.has("queryState") && !node.path("queryState").isNull())
                if (node.get("queryState").asText().equals("\"COMPLETED\"")) {
                    String replyTo = node.get("location").asText();
                    com.publishToDestination(msg, replyTo);
                } else {
                    System.out.println("query " + id + " is not completed to respont to ");
                }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Reply to results for the query    select url,count(pagerank) from webpages join entities on url=webpage group by domainName order by sum(pagerank)
    private void replyGetResults(String id) {
        TextMessage msg = new ActiveMQTextMessage();
        String query = (String) queriesCache.get(id);
        try {
            msg.setStringProperty("type", "results");

        } catch (JMSException e) {
            e.printStackTrace();
        }
        try {
            JsonNode root = mapper.readTree(query);
            ObjectNode node = (ObjectNode) root;
            String readFrom = mapper.readValue(node.path("output").textValue(), String.class);

            List<String> tuples = new ArrayList<String>();
            Map<String, String> resultSet = InfinispanUtils.getOrCreatePersistentMap(readFrom);
            if (!node.has("isSorted")) {
                for (Map.Entry<String, String> t : resultSet.entrySet()) {
                    tuples.add(t.getValue());
                }
            } else {
                int size = resultSet.size();
                for (int i = 0; i < size; i++) {
                    tuples.add((resultSet.get(readFrom + Integer.toString(i))));
                }
            }

            msg.setText(mapper.writeValueAsString(tuples));
            String replyTo = node.get("location").asText();
            com.publishToDestination(msg, replyTo);
            InfinispanUtils.removeCache(readFrom);
            msg = null;
            tuples.clear();
            node = null;
            tuples = null;
            queriesCache.remove(id);
            Runtime.getRuntime().gc();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JMSException e) {
            e.printStackTrace();
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
