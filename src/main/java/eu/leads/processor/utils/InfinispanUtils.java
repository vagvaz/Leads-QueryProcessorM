package eu.leads.processor.utils;

import eu.leads.crawler.concurrent.Queue;
import eu.leads.crawler.utils.Infinispan;
import org.infinispan.Cache;
import org.infinispan.InvalidCacheUsageException;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static java.lang.System.getProperties;

//import org.infinispan.atomic.AtomicObjectFactory;

/**
 * @author P. Sutra
 */
public class InfinispanUtils {

//    private static AtomicObjectFactory factory;
    private static DefaultCacheManager manager;
    private static volatile boolean isStarted = false;

    public static void start() {
        if(isStarted)
            return;

        String infinispanConfig = getProperties().getProperty("processorInfinispanConfigFile");

        if (infinispanConfig != null) {
            try {
                System.out.println("processorInfinispanConfigFile " + infinispanConfig);
                manager = new DefaultCacheManager(infinispanConfig);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Incorrect InfinispanUtils configuration file");
            }
        } else {
            try {
                System.out.println("default cache value configuration");
                manager = new DefaultCacheManager(System.getenv("HOME")+"/infinispan-clustered-tcp.xml");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        manager.start();
	System.out.println("infinispan started and synchronizing.\nPlease wait...");
    }

    public static void stop() {
        manager.stop();

    }

    public static synchronized ConcurrentMap getOrCreatePersistentMap(String name) {
        return manager.getCache(name);
    }



    public static void addListenerForMap(Object listener, ConcurrentMap map) {
        ((Cache) map).addListener(listener);
    }

    public static void addListenerForMap(Object listener, String cacheName) {
        Cache c = manager.getCache(cacheName, true);
        c.addListener(listener);
    }



    public static List<Address> getMembers() {
        return manager.getMembers();
    }

    public static void removeCache(String table) {
//        System.out.println("----\n\n\n\n Removing " + table + "\n\n\n\n");
        Cache c = manager.getCache(table);
        c.clear();
        c.stop();
//        if(manager.cacheExists(table))
//            manager.removeCache(table);

//        for(String s : manager.getCacheNames())
//        {
//            Cache cc = manager.getCache(s);
//            if(cc.getStatus().equals(ComponentStatus.RUNNING))
//            StdOutputWriter.getInstance().println(table+": " + s + " " + cc.size());
//        }

    }

    public static Address getMemberName() {
        return manager.getAddress();
    }


}
