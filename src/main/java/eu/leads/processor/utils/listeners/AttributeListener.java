package eu.leads.processor.utils.listeners;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.utils.AlchemyScore;
import eu.leads.processor.utils.InfinispanUtils;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/30/13
 * Time: 10:35 AM
 * To change this template use File | Settings | File Templates.
 */
@Listener
public class AttributeListener {
    private final String table;
    private final String attribute;
    private final String value;
    private final ConcurrentMap collection;
    private final ObjectMapper mapper;
    private final String keyName;

    public AttributeListener(String table, String attribute, String value, String collectionName) {
        this.table = table + ":";
        this.attribute = attribute;
        this.value = value;
        collection = InfinispanUtils.getOrCreatePersistentMap(collectionName + ":");
        mapper = new ObjectMapper();
        keyName = collectionName + ":";
        AlchemyScore.initialize();
    }

    @CacheEntryModified
    public void isMatching(CacheEntryModifiedEvent<Object, Object> event) {
        if (event.isPre())
            return;
//        System.out.println("Attr " + event.getKey().toString());
        String key = (String) event.getKey();
        if (key.startsWith(table)) {
            try {
//                System.out.println("Cheking reading " + key);
                JsonNode root = mapper.readTree((String) event.getValue());
                if (root.path(attribute).asText().toLowerCase().contains(value.toLowerCase())) {
                    String colKey = "";
                    String colValue = "";
                    String fkey = (String) event.getKey();
                    String url = fkey.substring(fkey.indexOf(":") + 1);
                    Tuple tuple = new Tuple("{}");
                    tuple.setAttribute("name", "adidas");
                    tuple.setAttribute("webpageURL", url);
                    Float f = AlchemyScore.getScore(url, "adidas");


                    if (f > -1) {
                        tuple.setAttribute("sentimentScore", mapper.writeValueAsString(f));
                        collection.put(keyName + url + "adidas", tuple.asString());
                    }


                }
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

}
