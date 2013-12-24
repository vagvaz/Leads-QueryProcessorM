package eu.leads.processor.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.crawler.model.Page;
import eu.leads.processor.execute.ProgressReport;
//import eu.leads.processor.pagerank.graph.LeadsPrGraph;
import org.infinispan.Cache;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Timer;
import java.util.Vector;

/**
 * Created by vagvaz on 12/23/13.
 */
public class ImportCrawledPages implements Runnable {

    private Thread thread;
    private String crawlerCache;
    private String destinationCache;
    public ImportCrawledPages(String src,String dest){
        crawlerCache = src;
        destinationCache = dest;
        thread = new Thread(this);
    }
    @Override
    public void run() {
        Cache<String, String> control = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap("processorControl");
        if(control.containsKey("service"))
        {
            System.out.println("Service is running so import is unnecessary");
            return;
        }
        importCrawledPages(crawlerCache,destinationCache);
        thread.interrupt();
        return;

    }

    public void start(){
        this.thread.start();
    }

    public void join(){
        try {
            this.thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private void importCrawledPages(String source, String destination) {
        AlchemyScore.initialize();
        Cache cache = (Cache) InfinispanUtils.getOrCreatePersistentMap(source);
        Cache webpagesCache = (Cache)InfinispanUtils.getOrCreatePersistentMap(destination);
        ObjectMapper mapper = new ObjectMapper();
        ProgressReport report = new ProgressReport("\nImported Tuples ",0,cache.size() );
        Timer timer = new Timer();
//        timer.scheduleAtFixedRate(report,10,2000);
        Vector<LeadsProcessorPage> buffer = new Vector<LeadsProcessorPage>(100);
        StdOutputWriter.getInstance().println("Importing " + cache.size() + " crawled pages");
        for(Object ob : cache.values()){
            Page page = (Page)ob;
            LeadsProcessorPage processorPage = new LeadsProcessorPage(page);
            Double sentiment = null;
            sentiment = AlchemyScore.getScore(page.getUrl().toString());
            report.tick();
            processorPage.setSentiment(sentiment);
            //put negative value for generating the pagerank value during query processing.
            //and let the Leads pagerank algorithm to process the webpage.
            try {
                processorPage.setPagerank((double) Web.pagerank("http://" + page.getUrl().toURI().getHost()));
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
            buffer.add(processorPage);
            if(buffer.size() == 100)
            {
                try {
                    for(LeadsProcessorPage procpage: buffer)
                        webpagesCache.put(procpage.getUrl(),mapper.writeValueAsString(procpage));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                buffer.clear();
            }


        }
        try {
            for(LeadsProcessorPage procpage: buffer)
                webpagesCache.put(procpage.getUrl(),mapper.writeValueAsString(procpage));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        buffer.clear();
//        report.run();
        report.cancel();
        timer.cancel();
    }

    public void stop() {
        thread.interrupt();
        thread = null;
    }
}
