package eu.leads.processor.utils.listeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.crawler.model.Page;
import eu.leads.processor.utils.AlchemyScore;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.LeadsProcessorPage;
import eu.leads.processor.utils.Web;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by vagvaz on 12/11/13.
 */
@Listener
public class TrackListener {

    private Cache collection = null;
    private static Log log = LogFactory.getLog(PrefixListener.class.getName());
    private final String target;
    private int counter = 0;
    private final ObjectMapper mapper;


    public TrackListener(String target) {
        this.target = target;
        if (collection == null)
            collection = (Cache) InfinispanUtils.getOrCreatePersistentMap(target);
        mapper = new ObjectMapper();
//        System.out.println("Initalized " + prefix);
    }

    @CacheEntryCreated
    public void isMatching(CacheEntryCreatedEvent<Object, Object> event) {
        if (event.isPre())
            return;
        URL url = null;
        try {
            url = new URL((String) event.getKey());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        Page page = (Page) event.getValue();

        //Use WP3 pagerank to process
//        try {
//            LeadsPrGraph.processCrawledPage(page);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        //Use custom page for saving page into json format
        LeadsProcessorPage procPage = new LeadsProcessorPage(page);

        Double sentiment = null;

        sentiment = getOverallSentimetForPage(page);
        procPage.setSentiment(sentiment);
        //put negative value for generating the pagerank value during query processing.
        try {
            procPage.setPagerank((double) Web.pagerank("http://" + url.toURI().getHost()));
        } catch (URISyntaxException e) {
            return; // if there was no pagerank then do not store the page.
        }

        try {
            collection.put(target + procPage.toString(), mapper.writeValueAsString(procPage));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return;
    }

    private Double getOverallSentimetForPage(Page page) {
        // Sentiment analysis
//        SentimentCall call = new SentimentCall(new CallTypeUrl(page.getUrl().toString()));
//        Response response = client.call(call);
//        SentimentAlchemyEntity entity = (SentimentAlchemyEntity) response.iterator().next();
//        return Double.parseDouble(entity.getScore().toString());
        return AlchemyScore.getScore(page.getUrl().toString());
    }

    public String getTarget() {
        return target;
    }
}
