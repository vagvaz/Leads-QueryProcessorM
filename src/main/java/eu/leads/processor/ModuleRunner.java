package eu.leads.processor;

import eu.leads.crawler.PersistentCrawl;
import eu.leads.processor.deploy.QueryDeployer;
import eu.leads.processor.deploy.QueryDeployerImpl;
import eu.leads.processor.execute.NodeQueryExecutor;
import eu.leads.processor.plan.QueryPlanner;
import eu.leads.processor.plan.QueryPlannerImpl;
import eu.leads.processor.ui.SQLInterface;
import eu.leads.processor.ui.UserInterfaceManager;
import eu.leads.processor.utils.InfinispanUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import static java.lang.System.getProperties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/6/13
 * Time: 10:45 PM
 * To change this template use File | Settings | File Templates.
 */
//Helper class which is currently not used for deploying specific module

public class ModuleRunner {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Please enter the name of class");
        }
        try {
            Properties properties = getProperties();
            properties.load(PersistentCrawl.class.getClassLoader().getResourceAsStream("config.properties"));
            System.out.println("Found properties file.");
        } catch (IOException e) {
            System.out.println("Found no config.properties file; defaulting.");
        }
        InfinispanUtils.start();
        String hostname = "";
        String ip = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        boolean found = true;
        String className = args[0];
        Class<Module> moduleClass = null;
        Module module = null;
        try {
            moduleClass = (Class<Module>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            found = false;

        }

        if (!found) {
            found = true;
            try {
                moduleClass = (Class<Module>) Class.forName("eu.leads.processor." + className);
            } catch (ClassNotFoundException e) {
                found = false;
                e.printStackTrace();
            }
        }
        String url = "tcp://" + ip + ":61616";
        System.out.println("cl: " + moduleClass.toString());
        String name = hostname + "." + className;
        if (moduleClass.equals(SQLInterface.class)) {
            try {
                module = new SQLInterface(url, name);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (moduleClass.equals(UserInterfaceManager.class)) {
            try {
                module = new eu.leads.processor.ui.UserInterfaceManager(url, name);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (moduleClass.equals(QueryPlanner.class)) {
            try {
                module = new QueryPlannerImpl(url, name);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (moduleClass.equals(QueryDeployer.class)) {
            try {
                module = new QueryDeployerImpl(url, name);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else if (moduleClass.equals(NodeQueryExecutor.class)) {
            try {
                module = new NodeQueryExecutor(url, name);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("No valid Module to start bye bye");
            System.exit(-1);
        }
        if(module != null) {
        module.startUp();
        }
        InfinispanUtils.stop();
    }
}
