package eu.leads.processor.deploy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import eu.leads.processor.Module;
import eu.leads.processor.execute.operators.OperatorCompletionEvent;
import eu.leads.processor.execute.operators.OperatorType;
import eu.leads.processor.execute.operators.ReadOperator;
import eu.leads.processor.plan.*;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.query.QueryState;
import eu.leads.processor.sql.PlanNode;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.StdOutputWriter;
import eu.leads.processor.utils.StringConstants;
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
 * Date: 10/29/13
 * Time: 10:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class LeadsDeployerService extends Module {
    private EventBus eventBus;
    private Queue<Message> incoming;
    private ObjectMapper mapper;
    private final Object mutex = new Object();
    private Set<String> ignore;
    private Map<String, ExecutionPlanInfo> info;
    private ConcurrentMap queries;
    private final String executorQueue = StringConstants.NODEEXECUTORQUEUE;
    private final String deployerQueue = StringConstants.DEPLOYERQUEUE;
    private final String userInterfaceMQ = StringConstants.UIMANAGERQUEUE;

    public LeadsDeployerService(String url, String name) throws Exception {
        super(url, name);
        com.subscribeToQueue(deployerQueue);
        com.createQueuePublisher(executorQueue);
        com.setQueueMessageListener(this);
        eventBus = null;
        incoming = new LinkedList<Message>();
        mapper = new ObjectMapper();
        ignore = new HashSet<String>();
        ignore.add(OperatorType.toString(OperatorType.READ));
//        ignore.add(OperatorType.toString(OperatorType.OUTPUT));
        info = new HashMap<String, ExecutionPlanInfo>();
        queries = InfinispanUtils.getOrCreatePersistentMap(StringConstants.QUERIESCACHE);

    }

    public LeadsDeployerService(String url, String name, EventBus eventBus) throws Exception {
        super(url, name);
        this.eventBus = eventBus;
        this.eventBus.register(this);
        com.subscribeToQueue(deployerQueue);
        com.createQueuePublisher(executorQueue);
        com.setQueueMessageListener(this);
        incoming = new LinkedList<Message>();
        mapper = new ObjectMapper();
        //Ignore operators that should not be deployed.
        ignore = new HashSet<String>();
        ignore.add(OperatorType.toString(OperatorType.READ));
        ignore.add(OperatorType.toString(OperatorType.OUTPUT));
        info = new HashMap<String, ExecutionPlanInfo>();
        queries = InfinispanUtils.getOrCreatePersistentMap(StringConstants.QUERIESCACHE);
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
                    if (type.equals("execute")) {
                        //Read Query Context from message
                        QueryContext context = mapper.readValue(message.getStringProperty("context"), QueryContext.class);
                        StdOutputWriter.getInstance().println("QueryDeployer received an execution plan for query " + context.getQueryId());
                        String planString = message.getText();
                        ExecutionPlan plan = mapper.readValue(planString, ExecutionPlan.class);
                        initializeExecution(plan, context);
                        startExecution(plan, context);
                    }
                }
            }

        }
    }

    //Start the execution of the plan
    private void startExecution(ExecutionPlan plan, QueryContext context) {
        Collection<String> sources = plan.getSources();
        //For all the source nodes in the plan deploy
        for (String source : sources) {
            ExecutionPlanNode node = (ExecutionPlanNode) plan.getNode(source);
            // A read operator is not Deployed, since we just read from the appropriate KVS cache
            //thus we complete and try to get the next operator.
            if (node.getOperatorType().equals(OperatorType.READ)) {
                node.setStatus(NodeStatus.COMPLETED);
                node = getNextOperator(info.get(context.getQueryId()), node.getName());
            }
            //The next operator is not ready to be deployed (not all the inputs are completed).
            if (node == null)
                continue;
            //When we come across the output operator then  we complete the query
            //otherwise we deploy the operator.
            if (node.getOperatorType().equals(OperatorType.OUTPUT)) {
                completeQuery(info.get(context.getQueryId()));
            } else {
                deploy(node, info.get(context.getQueryId()));
            }
        }
    }

    //Initialize the Execution of a plan
    private void initializeExecution(ExecutionPlan plan, QueryContext context) {
        //Create an ExecutionPlanInfo which tracks the listeners and structures created for executing an execution plan.
        ExecutionPlanInfo planInfo = new ExecutionPlanInfo(plan, context);
        info.put(context.getQueryId(), planInfo);

        Collection<PlanNode> col = plan.getNodes();
//        Iterator<PlanNode> iterator = col.iterator();
        //Inform monitor service for the initialization of the execution plan.
        InitializeExecutionPlanEvent event = new InitializeExecutionPlanEvent();
        eventBus.post(event);
    }

    //Event handler to handle the completion of an operator in order to deploy the next operator.
    @Subscribe
    public void operatorCompletion(OperatorCompletionEvent e) {
        ExecutionPlanInfo planInfo = info.get(e.getQueryId());
        ExecutionPlanNode node = (ExecutionPlanNode) planInfo.getPlan().getNode(e.getOperator());
        node.setStatus(NodeStatus.COMPLETED);
        //If there is a sort operator then we must update the query structure in the queriesCache
        //We must know that a query has a sorted result in order to present tuples with the correct order
        if (node.getOperatorType().equals(OperatorType.SORT)) {
            String query = (String) queries.get(planInfo.getContext().getQueryId());
            try {
                //Update query structure in order to read sorted results in the correct sequence
                JsonNode root = mapper.readTree(query);
                ObjectNode nodeObject = (ObjectNode) root;
                nodeObject.put("isSorted", mapper.writeValueAsString(true));
                queries.put(planInfo.getContext().getQueryId(), nodeObject.toString());
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        //deploy next operator
        node = getNextOperator(planInfo, e.getOperator());
        deploy(node, planInfo);
    }

    //Get next operator for the operator in the plan stored in planInfo
    private ExecutionPlanNode getNextOperator(ExecutionPlanInfo planInfo, String operator) {
        ExecutionPlanNode node = (ExecutionPlanNode) planInfo.getPlan().getNode(operator);
        node = (ExecutionPlanNode) planInfo.getPlan().getNode(node.getOutput());
        //if there is the output operator then we have completed the plan execution.
        if (OperatorType.fromString(node.getType()) == OperatorType.OUTPUT) {
            completeQuery(planInfo);
            return null;
        } else {
            //check if all the node sources have been completed if not return null
            for (String source : node.getSources()) {
                ExecutionPlanNode inputNode = (ExecutionPlanNode) planInfo.getPlan().getNode(source);
                if (inputNode.getStatus() != NodeStatus.COMPLETED)
                    return null;
            }
        }
        return node;
    }

    //Deploy operator node
    private void deploy(ExecutionPlanNode node, ExecutionPlanInfo planInfo) {
        if (node == null)
            return;
        TextMessage message = new ActiveMQTextMessage();
        ArrayList<String> inputs = null;
        //Resolve input in order to pass the correct input cache argument to the operator
        try {
            inputs = resolveInput(node, planInfo);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //Right now this is necessary for join operator where there are 2 inputs.
        StringBuilder builder = new StringBuilder();
        if(inputs != null){
        for (String input : inputs)
            builder.append(input + ",");
        }
        try {
            //Prepare JMS message send it and set the node state to RUNNING
            String input = builder.toString();
            input = input.substring(0, input.length() - 1);
            message.setStringProperty("input", input);
            message.setStringProperty("type", "executeOperator");
            message.setStringProperty("queryId", planInfo.getContext().getQueryId());
            message.setText(mapper.writeValueAsString(node));
            com.publishToQueue(message, executorQueue);
            node.setStatus(NodeStatus.RUNNING);
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    //Resolve Input
    private ArrayList<String> resolveInput(ExecutionPlanNode node, ExecutionPlanInfo planInfo) {
        ArrayList<String> result = new ArrayList<String>();
        List<String> sources = node.getSources();
        for (String source : sources) { //for each input of the node
            ExecutionPlanNode inputNode = (ExecutionPlanNode) planInfo.getPlan().getNode(source);
            //if READ Operator then the input is the name of the table
            if (inputNode.getType().equals(OperatorType.toString(OperatorType.READ))) {
                ReadOperator read = (ReadOperator) inputNode;
                result.add(read.getTable().getName());
            } else { // otherwise the input is the name of the operator.
                result.add(inputNode.getName());
            }
        }
        return result;
    }

    //Complete Query
    //Update query structure stored in KVS to store the appropriate output cache and
    //set the query state to Completed
    private void completeQuery(ExecutionPlanInfo planInfo) {
        planInfo.finalizePlan();
        String value = (String) queries.get(planInfo.getContext().getQueryId());
        try {
            JsonNode root = mapper.readTree(new StringReader(value));
            ((ObjectNode) root).put("queryState", mapper.writeValueAsString(QueryState.COMPLETED));
            ((ObjectNode) root).put("output", mapper.writeValueAsString(planInfo.getPlan().getOutput().getSources().get(0) + ":"));
            queries.put(planInfo.getContext().getQueryId(), root.toString());
            //inform user interface manager that the query has been processed
            sendMessageToUIManager(planInfo.getContext().getQueryId());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendMessageToUIManager(String queryId) {
        TextMessage msg = new ActiveMQTextMessage();
        try {
            msg.setText(queryId);
            msg.setStringProperty("type", "queryCompletion");
//            System.err.println("sending to user Interface ");
            com.publishToDestination(msg, userInterfaceMQ);

        } catch (JMSException e) {
            e.printStackTrace();
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
