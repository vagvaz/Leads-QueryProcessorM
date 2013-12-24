package eu.leads.processor.deploy;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import eu.leads.processor.Module;
import eu.leads.processor.execute.operators.OperatorCompletionEvent;
import eu.leads.processor.plan.InitializeExecutionPlanEvent;
import eu.leads.processor.utils.StringConstants;

import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 10:44 AM
 * To change this template use File | Settings | File Templates.
 */
public class LeadsMonitorService extends Module {
    private EventBus eventBus;
    private LinkedList<Message> incoming;
    private final Object mutex = new Object();
    private LeadsDeployerService deployer =null;

    public LeadsMonitorService(String url, String name) throws Exception {
        super(url, name);
        incoming = new LinkedList<Message>();
    }

    public LeadsMonitorService(String url, String name, EventBus eventBus,LeadsDeployerService deploy) throws Exception {
        super(url, name);
        this.eventBus = eventBus;
        incoming = new LinkedList<Message>();
        com.subscribeToQueue(StringConstants.DEPLOYERQUEUE + ".monitor");
        com.setQueueMessageListener(this);
        incoming = new LinkedList<Message>();
        eventBus.register(this);
        this.deployer = deploy;
    }


    @Subscribe
    public void monitorExecution(InitializeExecutionPlanEvent e) {
        return;
    }

    //At the moment  Monitor service just listens for operator completion and informs deployer service
    @Override
    protected void run() throws Exception {
        while (isRunning()) {
            LinkedList<Message> unprocessed = null;
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
                    TextMessage message = (TextMessage) unprocessed.remove(0);
                    String type = message.getStringProperty("type");
                    if (type.equals("completion")) {
                        String queryId = message.getStringProperty("queryId");
                        String operator = message.getText();
                        OperatorCompletionEvent event = new OperatorCompletionEvent(operator, queryId);
                        deployer.operatorCompletion(event);
                    } else {
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
