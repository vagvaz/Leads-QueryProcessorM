package eu.leads.processor.ui;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.crawler.model.Page;
import eu.leads.crawler.utils.Infinispan;
import eu.leads.processor.conf.WP3Configuration;
import eu.leads.processor.deploy.QueryDeployerImpl;
import eu.leads.processor.execute.NodeQueryExecutor;
import eu.leads.processor.execute.ProgressReport;
import eu.leads.processor.execute.TableInfo;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.plan.QueryPlannerImpl;
import eu.leads.processor.utils.*;
import eu.leads.processor.utils.listeners.AttributeListener;
import eu.leads.processor.utils.listeners.TrackListener;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.ServiceStopper;
import org.infinispan.Cache;


import java.io.EOFException;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by vagvaz on 12/10/13.
 */
public class SQLInterfaceBootstrap {
    private static final String[] columnNames = {"url", "domainName", "pagerank", "body", "sentiment", "published", "links"};
    private static final String[] columnType = {"string", "string", "double", "string", "double", "date", "[string]"};
    private static final String[] columnNamesB = {"webpageURL", "name", "sentimentScore"};
    private static final String[] columnTypeB = {"string", "string", "double"};
    private static final int numOfTuples = 10000;


    public static void main(String[] args) {
        try {
            //Initialize configuration
            WP3Configuration.initialize();
            boolean started = true;

            //start JMS Broker
            BrokerService service = tryToStartJMS();

            if (service == null) {
                System.err.println("Failed To Start JMS check if you are already running a JMS Queue");
                System.exit(-1);
            }


            String connectURI = service.getTransportConnectors().get(0).getConnectUri().toString();
            System.out.println("start JMS Broker " + connectURI);

            //Start infinispan
            InfinispanUtils.start();

            ImportCrawledPages importer = new ImportCrawledPages(WP3Configuration.getProperty("crawlerCache"),"webpages:");
            importer.start();
            //Start NodeQueryExecutor module
            NodeQueryExecutor ex = new NodeQueryExecutor(connectURI, "NQE:" + WP3Configuration.getNodeName());
            ex.startAsync();
            ex.awaitRunning();

            //Start QueryDeployer module
            QueryDeployerImpl qd = new QueryDeployerImpl(connectURI, "QD:" + WP3Configuration.getNodeName());
            qd.startAsync();
            qd.awaitRunning();

            //Start QueryPlanner module
            QueryPlannerImpl qp = new QueryPlannerImpl(connectURI, "QP:" + WP3Configuration.getNodeName());
            qp.startAsync();
            qp.awaitRunning();

            //Start user interface manager
            UserInterfaceManager uim = new UserInterfaceManager(connectURI, "UIM:" + WP3Configuration.getNodeName());
            uim.startAsync();
            uim.awaitRunning();


//TODO            Cache<String, String> data = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap("webpages");
            //Read The string of the crawler configuration

            String crawlerCache = WP3Configuration.getProperty("crawlerCache");
            //Override from command line argument
            if (args.length > 0)
                crawlerCache = args[0];

            //Get Cache and add a listener to track when an entry is inserted.
            Cache<String, String> crCache = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap(crawlerCache);
            TrackListener listener = new TrackListener("webpages:");
            crCache.addListener(listener);
            System.out.println("Initalize listener for webpages");

            //Add a listener whenever a webpage is inserted in order to process it for
            //interesting entities
//TODO            PrefixListener prefixListenerB = new PrefixListener("entities:");
//TODO            InfinispanUtils.addListenerForMap(prefixListenerB, "webpages:");

            System.out.println("Initalize listener for entities");
            AttributeListener attributeListener = new AttributeListener("webpages", "body", "adidas", "entities");
            InfinispanUtils.addListenerForMap(attributeListener, "webpages:");
            System.out.println("Initialize listener for webpages.body like \'adidas\'");

            PersistentCrawlingModule crawler = null;
            if (WP3Configuration.getBoolean("crawl")) {
                crawler = new PersistentCrawlingModule(connectURI, "CR:" + WP3Configuration.getNodeName());
                crawler.startAsync();
                crawler.awaitRunning();
            }

            ConcurrentMap<String,String> data = InfinispanUtils.getOrCreatePersistentMap("entities:");
            ConcurrentMap<String,String> web = InfinispanUtils.getOrCreatePersistentMap("webpages:");

            //populate with random data
            if(WP3Configuration.getBoolean("populateWithRandomData"))
                populateTableWithRandomData(numOfTuples);

            //generate wepbages and entity tables
            generateTables();

            //start sqlInterface
            SQLInterface sqlInterface = new SQLInterface(connectURI, "SQLInterface:" + WP3Configuration.getNodeName());
            sqlInterface.startAsync();

//            while (sqlInterface.state() != Service.State.TERMINATED)
//                Thread.sleep(10000);
            //stop modules
            sqlInterface.awaitTerminated();
            if (WP3Configuration.getBoolean("crawl")) {
                crawler.stopAsync();
                crawler.awaitTerminated();
            }
            qp.stopAsync();
            qp.awaitTerminated();
            uim.stopAsync();
            uim.awaitTerminated();
            qd.stopAsync();
            qd.awaitTerminated();
            ex.stopAsync();
            ex.awaitTerminated();
            importer.stop();
            try {
                //stop JMS Broker
                if (started) {
                    service.stopAllConnectors(new ServiceStopper());
                    service.stop();
                }
            } catch (EOFException e) {

            }

            service.waitUntilStopped();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        //sto InfinispanUtils Manager
        InfinispanUtils.stop();
        StdOutputWriter.stop();

        System.out.println("Thanks for using...");
        System.exit(0);
    }

    private static void populateTableWithRandomData(int numOfTuples) {
        Map<String,String> entities = InfinispanUtils.getOrCreatePersistentMap("entities:");
        Map<String,String> web = InfinispanUtils.getOrCreatePersistentMap("webpages:");
        for (int i = 0; i < numOfTuples/100; i++) {
            Tuple t = Utilities.generateTuple(columnNamesB, columnTypeB);

            entities.put("entities:" + (Integer.toString(i)) + t.asString(), t.asString());
        }


        for (int i = 0; i < numOfTuples; i++) {
            Tuple t = Utilities.generateTuple(columnNames,columnType);
            web.put("webpages:"+(Integer.toString(i))+t.asString(),t.asString());
        }
    }



    private static void generateTables() {
        ObjectMapper mapper = new ObjectMapper();
        CCJSqlParserManager manager = new CCJSqlParserManager();
        //put webpages table info into infinispan
        Statement s = null;
        try {
            s = manager.parse(new StringReader("CREATE TABLE webpages(url varchar(100),domainName varchar(100),pagerank double, body varchar(100),sentiment double,published date, links varchar(1000))"));
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        Map<String, String> tables = InfinispanUtils.getOrCreatePersistentMap("tables");
        CreateTable ct = (CreateTable) s;
        TableInfo ti = null;
        if(ct != null) {

            ti = new TableInfo(ct.getTable(), (List<String>) ct.getTableOptionsStrings(), ct.getColumnDefinitions());
        }
        try {
            tables.put("tables:" + ti.getTable().getName(), mapper.writeValueAsString(ti));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }


        //put entities table into infinispan
        try {
            s = manager.parse(new StringReader("CREATE TABLE entities(webpageURL varchar(100),name varchar(100),sentimentScore double)"));
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
        ct = (CreateTable) s;
        ti = new TableInfo(ct.getTable(), (List<String>) ct.getTableOptionsStrings(), ct.getColumnDefinitions());
        try {
            tables.put("tables:" + ti.getTable().getName(), mapper.writeValueAsString(ti));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }

    private static BrokerService tryToStartJMS() {
        boolean started = true;
        BrokerService service = new BrokerService();
        int portrange = 20;
        int staringport = 61616;
        int tries = 0;
        int step = 1;
        for (tries = 0; tries < portrange; tries++) {
            try {
                String connectURI = "tcp://" + WP3Configuration.getHostname() + ":" + Integer.toString(staringport + tries * step);
                service.addConnector(connectURI);
                service.start();
                service.waitUntilStarted();
                started = true;
                break;
            } catch (IOException e) {
                started = false;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (!started)
            return null;
        return service;
    }
}
