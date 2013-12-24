package eu.leads.processor.utils;

import eu.leads.crawler.*;
import eu.leads.crawler.concurrent.Queue;
import eu.leads.crawler.download.DefaultDownloader;
import eu.leads.crawler.download.DefaultDownloaderController;
import eu.leads.crawler.download.DefaultProxyController;
import eu.leads.crawler.parse.DefaultParser;
import eu.leads.crawler.parse.DefaultParserController;
import eu.leads.crawler.utils.Infinispan;
import eu.leads.processor.Module;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Message;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


import static java.lang.System.getProperties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/7/13
 * Time: 4:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class PersistentCrawlingModule extends Module {
    private static final Log log = LogFactory.getLog(PersistentCrawl.class.getName());
    CrawlerController crawlerController;

    public PersistentCrawlingModule(String url, String name) throws Exception {
        super(url, name);
    }

    @Override
    protected void run() throws Exception {

        List<Proxy> proxies = new ArrayList<Proxy>();
        String seed = "http://www.economist.com/";
        ArrayList<String> words = new ArrayList<String>();
        int ncrawlers = 1;
        int ndays = 31;

        try{
            Properties properties = getProperties();
            properties.load(PersistentCrawl.class.getClassLoader().getResourceAsStream("config.properties"));
            log.info("Found properties file.");
        } catch (IOException e) {
            log.info("Found no config.properties file; defaulting.");
        }

        if(getProperties().containsKey("seed")){
            seed = getProperties().getProperty("seed");
            log.info("Seed : " + seed);
        }

        if(getProperties().containsKey("words")){
            for(String w : getProperties().get("words").toString().split(",")){
                log.info("Adding word :"+w);
                words.add(w);
            }
        }else{
            words.add("Obama");
        }

        Infinispan.start();

        if(getProperties().containsKey("ncrawlers")){
            ncrawlers = Integer.valueOf(getProperties().getProperty("ncrawlers"));
            log.info("Using "+ncrawlers+" crawler(s)");
        }

        if(getProperties().containsKey("ndays")){
            ndays = Integer.valueOf(getProperties().getProperty("ndays"));
            log.info("Document earler than "+ndays+" day(s)");
        }

        proxies.add(Proxy.NO_PROXY);
        DefaultProxyController proxyController = new DefaultProxyController(proxies);

        DefaultDownloader downloader = new DefaultDownloader();
        downloader.setAllowedContentTypes(new String[]{"text/html", "text/plain"});
        downloader.setMaxContentLength(100000);
        downloader.setTriesCount(3);
        downloader.setProxyController(proxyController);

        DefaultDownloaderController downloaderController = new DefaultDownloaderController();
        downloaderController.setGenericDownloader(downloader);

        DefaultParserController defaultParserController = new DefaultParserController();
        defaultParserController.setGenericParser(DefaultParser.class);

        CrawlerConfiguration configuration = new CrawlerConfiguration();
        configuration.setMaxHttpErrors(HttpURLConnection.HTTP_BAD_GATEWAY, 10);
        configuration.setMaxLevel(3);
        configuration.setMaxParallelRequests(5);
        configuration.setPolitenessPeriod(500);

        try {

//            PersistentListener listener = new PersistentListener(words,ndays);

            for (int i = 0; i < ncrawlers; i++) {
                PersistentCrawler crawler = new PersistentCrawler();
                crawler.setDownloaderController(downloaderController);
                crawler.setParserController(defaultParserController);
                configuration.addCrawler(crawler);
            }

            crawlerController = new CrawlerController(configuration);

            Queue q = Infinispan.getOrCreateQueue("queue");
            log.info(q.size());
            crawlerController.setQueue(q);
            if(!seed.equals("") && q.size()==0 ) crawlerController.addSeed(new URL(seed));
            crawlerController.start();
            crawlerController.join();

        } catch (Exception e) {
            e.printStackTrace();
        }

        Infinispan.stop();

        System.out.println("Terminated.");

        while (isRunning()) ;
    }

    @Override
    public void onMessage(Message message) {

    }

    @Override
    public void triggerShutdown() {
        super.triggerShutdown();

        try {
            com.disable();
            crawlerController.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
