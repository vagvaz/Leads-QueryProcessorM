package eu.leads.processor.ui;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import eu.leads.processor.Module;
import eu.leads.processor.conf.WP3Configuration;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.plan.SelectExtractor;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.sql.Plan;
import eu.leads.processor.utils.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;

import javax.jms.*;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/7/13
 * Time: 1:04 PM
 */
public class SQLInterface extends Module {

    static final Log log = LogFactory.getLog(SQLInterface.class.getName());
    private CCJSqlParserManager validator;
    private String user;
    private String location;
    private final String queue = StringConstants.UIMANAGERQUEUE;
    private LinkedList<Message> incoming;
    private HashMap<String, String> completed;
    private final Object mutex = new Object();
    private String ackQueryId;
    private String ackQueryText;
    private ArrayList<Tuple> resultSet;
    private boolean isCompleted;
    private Destination destination;
    private boolean receivedResults = false;

    public SQLInterface(String url, String name) throws Exception {
        //Initialize Module class
        super(url, name + "." + WP3Configuration.getNodeName());
        location = WP3Configuration.getHostname();
        com.createQueuePublisher(queue);
        validator = new CCJSqlParserManager();
        user = "default user";
        com.setTopicMessageListener(this);
        com.setQueueMessageListener(this);
        incoming = new LinkedList<Message>();
        completed = new HashMap<String, String>();
        resultSet = new ArrayList<Tuple>();
        ackQueryId = "";
        ackQueryText = "";
        isCompleted = false;
        destination = null;
    }

    @Override
    protected void run() throws Exception {


        StringBuilder input = null;
        CommandLineUtil console = new CommandLineUtil();
        user = console.read("Enter user name:");
        com.subscribeToQueue(user + "@" + location);

        while (isRunning()) {
            input = new StringBuilder();
            String line = console.read("Enter your SQL query");

            line = line.trim();
            input.append(line);

            while (!line.trim().endsWith(";") && !input.toString().endsWith(";")) {
                line = console.readLine();
                input.append(" " + line.trim());
            }


            StdOutputWriter.getInstance().println("");
            String query = input.toString();

            //Quit the SQLInterface
            if (query.toLowerCase().trim().equals("quit;")) {
                triggerShutdown();
                continue;
            }

            List<Address> addresses = InfinispanUtils.getMembers();
            StdOutputWriter.getInstance().write("Using KVS:");
            for (Address a : addresses) {
                StdOutputWriter.getInstance().write(((JGroupsAddress) a).toString() + " ");
            }

            //Show the plan for the query
            //Since there is no optimization in the query planner right now
            //and the basic plan will always be used  we can do it here.
            StdOutputWriter.getInstance().println("");
            if (query.trim().toLowerCase().startsWith("explain plan for ")) {
                String q = query.trim().replace("explain plan for ", "");

                //Parse Query and convert it to json
                CCJSqlParserManager manager = new CCJSqlParserManager();
                Statement s = manager.parse(new StringReader(q));
                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writeValueAsString(s);
                JsonNode root = mapper.readTree(json);
                //Extract basic plan
                SelectExtractor extractor = new SelectExtractor(root);
                QueryContext context = new QueryContext();
                Plan p = extractor.extractPlan("output", context);
                System.out.println("The plan is \n" + p.toString());
            }
            if (this.validateSQL(query)) {
//                console.show("your query was\n" + query);
                submitQuery(query);
                waitForAck();
                waitForCompletion();
                getResults(ackQueryId);
                ackQueryText = "";
                ackQueryId = "";
                isCompleted = false;
                resultSet.clear();
            } else {
                System.out.println("This is not an SQL query we can run please read Deliverable 3.2 instructuions");
//                input = new StringBuilder();
            }
        }
        console.show("Good Bye bye");
    }

    //Wait query to complete
    private void waitForCompletion() {
        while (true) {
            synchronized (mutex) {
                if (isCompleted)
                    break;
                try {
                    mutex.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    mutex.notify();
                }
            }
        }
    }

    //Poll User Interface manager for completion
    private void askForcompletion() {
        TextMessage message = new ActiveMQTextMessage();
        try {
            message.setText(ackQueryId);
            message.setStringProperty("type", "isCompleted");
            message.setStringProperty("user", user);
            message.setStringProperty("location", location);
            message.setJMSReplyTo(destination);
            message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            this.com.publishToQueue(message, queue);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    //Get the result for the submitted query.
    private void getResults(String ackQueryId) {
        TextMessage message = new ActiveMQTextMessage();
        try {
            message.setText(ackQueryId);
            message.setStringProperty("type", "getResults");
            message.setStringProperty("user", user);
            message.setStringProperty("location", location);
            message.setJMSReplyTo(destination);
            message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
            this.com.publishToQueue(message, queue);
            message = null;
        } catch (JMSException e) {
            e.printStackTrace();
        }
        synchronized (mutex) {
            if (!receivedResults) {
                try {
                    mutex.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        receivedResults = false;

        System.out.println("\n\nPresent Results for Query " + ackQueryId + "\n\n\n");
        System.out.flush();
        System.err.flush();
        printResults(resultSet);
    }

    //Print the results of the query
    private void printResults(ArrayList<Tuple> resultSet) {
        boolean firstTuple = true;
        if (resultSet.size() == 0) {
            System.out.println("EMPTY RESULTS");
            return;
        }
        int length = resultSet.size();
        int width = resultSet.get(0).getFieldSet().size();
        String[][] outputTable = new String[length + 1][width];
        Set<String> fields = resultSet.get(0).getFieldSet();
        int rowCount = 0;
        int colCount = 0;

        //Read fields
        for (String field : fields) {
            outputTable[rowCount][colCount] = field;
            colCount++;
        }

        for (Tuple t : resultSet) {
            rowCount++;
            colCount = 0;
            for (String field : fields) {
                outputTable[rowCount][colCount] = t.getAttribute(field);
                colCount++;
            }
        }
        //Show results to System out
        PrettyPrinter printer = new PrettyPrinter(System.out);
        printer.print(outputTable);
        resultSet.clear();
        printer = null;
//        outputTable = null;

    }

    //Wait until acknowledgement
    private void waitForAck() {
        synchronized (mutex) {
            if (Strings.isNullOrEmpty(ackQueryId) && Strings.isNullOrEmpty(ackQueryText)) {
                try {
                    mutex.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void submitQuery(String query) throws JMSException {
        TextMessage message = new ActiveMQTextMessage();
        message.setText(query);
        message.setStringProperty("user", user);
        message.setStringProperty("location", location);
        message.setStringProperty("type", "SQLQueryMessage");
        destination = com.getSession().createQueue(user + "@" + location);
        com.subscribeToQueue(destination.toString());
        com.setQueueMessageListener(this, destination.toString());
        message.setJMSReplyTo(destination);
        message.setJMSRedelivered(false);
        message.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        this.com.publishToQueue(message, queue);

    }

    public boolean validateSQL(String query) {
        try {
            StringReader reader = new StringReader(query);
            validator.parse(reader);
            return true;
        } catch (JSQLParserException e) {
            log.warn(e.toString());
            return false;
        }

    }

    @Override
    public void onMessage(Message message) {

        synchronized (mutex) {
            if (message != null) {
                TextMessage msg = (TextMessage) message;
                try {
                    if (msg.getStringProperty("type").equals("ack")) {
                        ackQueryId = msg.getText();
                        ackQueryText = msg.getStringProperty("sqlText");
                        mutex.notifyAll();
                    } else if (msg.getStringProperty("type").equals("completion")) {
                        String queryId = msg.getText();
                        if (queryId.equals(ackQueryId)) {
                            isCompleted = true;
                        }
                        mutex.notifyAll();
                    } else if (msg.getStringProperty("type").equals("results")) {

                        receivedResults = true;
                        String results = msg.getText();
                        ObjectMapper mapper = new ObjectMapper();
                        List<String> set = mapper.readValue(results, new TypeReference<List<String>>() {
                        });
                        if (set.size() == 0) {
                            mutex.notifyAll();
                            return;
                        }
                        for (String s : set)
                            resultSet.add(new Tuple(s));
                        mutex.notifyAll();
                    }
                    message.clearBody();
                    message = null;
                } catch (JMSException e) {
                    e.printStackTrace();
                    mutex.notifyAll();
                } catch (JsonMappingException e) {
                    e.printStackTrace();
                    mutex.notifyAll();

                } catch (JsonParseException e) {
                    e.printStackTrace();
                    mutex.notifyAll();
                } catch (IOException e) {
                    e.printStackTrace();
                    mutex.notifyAll();
                }
            }
        }
    }

    @Override
    protected void triggerShutdown() {
        super.triggerShutdown();
    }
}
