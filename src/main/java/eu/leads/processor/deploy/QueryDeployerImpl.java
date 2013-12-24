package eu.leads.processor.deploy;

import com.google.common.eventbus.EventBus;
import eu.leads.processor.Module;

import javax.jms.Message;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 10:38 AM
 * To change this template use File | Settings | File Templates.
 */
public class QueryDeployerImpl extends Module {
//Query Deployer has three modules/services
//    a. Deployer Service that is responsible for initializing and deploying plans and operators
//    b. Monitor Service that monitors the execution of a plans and operators (right now is fairly simple operators)
//    c. Recovery Service that recovers in case of failures (**unimplemented**)

    private LeadsDeployerService deployer; //Deployer Service
    private LeadsMonitorService monitor; // Monitor  Service
    private final Object mutex = new Object();

    public QueryDeployerImpl(String url, String name) throws Exception {
        super(url, name);
        //Initialize Event bus for communication between services
        EventBus eventBus = new EventBus();

        //Start deployer & monitor services
        deployer = new LeadsDeployerService(url, name + ".deployer", eventBus);
        monitor = new LeadsMonitorService(url, name + ".monitor", eventBus,deployer);

        deployer.startAsync();
        monitor.startAsync();
        deployer.awaitRunning();
        monitor.awaitRunning();


    }

    @Override
    protected void run() throws Exception {
        while (isRunning()) {
            //Right now QueryDeployer does not need to do anything.
            //Later should coordinate with the rest QueryDeployers.
            synchronized (mutex) {
                mutex.wait();
                break;
            }
        }
    }

    @Override
    public void onMessage(Message message) {

    }
//    RecoveryService recovery; // Recovery Service

    @Override
    protected void triggerShutdown() {
        synchronized (mutex) {
            mutex.notify();
        }
        super.triggerShutdown();
        deployer.triggerShutdown();
        monitor.triggerShutdown();
        deployer.awaitTerminated();
        monitor.awaitTerminated();
    }

}
