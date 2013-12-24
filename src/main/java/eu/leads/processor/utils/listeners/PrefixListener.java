package eu.leads.processor.utils.listeners;

import eu.leads.processor.utils.InfinispanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/30/13
 * Time: 10:23 AM
 * To change this template use File | Settings | File Templates.
 */
@Listener
public class PrefixListener {
    private Cache collection = null;
    private static Log log = LogFactory.getLog(PrefixListener.class.getName());
    private final String prefix;
    private int counter = 0;


    public PrefixListener(String prefix) {
        this.prefix = prefix;
        if (collection == null)
            collection = (Cache) InfinispanUtils.getOrCreatePersistentMap(prefix);

//        System.out.println("Initalized " + prefix);
    }

    @CacheEntryCreated
    public void isMatching(CacheEntryCreatedEvent<Object, Object> event) {
        if (event.isPre())
            return;
        String key = (String) event.getKey();
//       System.err.println(prefix + "invoked for " + key + " starts with  " + prefix + " " +(key.startsWith(prefix)) + " " +  collection.size()  );
        if (key.startsWith(prefix)) {
            if (counter < 10) {
//                System.out.println("adding to " + prefix +" key "+key);
                counter++;
            }
            collection.put(event.getKey(), event.getValue());
        }
//        System.out.println("finished " + prefix + "invoked for " + key + " starts with  " + prefix + " " +(key.startsWith(prefix)) + " " +  collection.size()  );
        return;
    }

    public String getPrefix() {
        return prefix;
    }
}
