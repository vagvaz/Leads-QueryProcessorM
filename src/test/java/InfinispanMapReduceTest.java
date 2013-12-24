import eu.leads.processor.utils.InfinispanUtils;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 5:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class InfinispanMapReduceTest {
    static class MyMapper implements Mapper<String, String, String, String> {
        final Properties conf;
        private static final long serialVersionUID = -5943370243108735561L;

        public MyMapper(Properties p) {
            conf = p;
        }

        @Override
        public void map(String s, String s2, Collector stringCollector) {
            stringCollector.emit(conf.getProperty("prefix") + s, s2);
        }
    }

    static class MyReducer implements Reducer<String, String> {
        final Properties conf;
        boolean isInitialized = false;
        int index = 0;
        final ConcurrentMap<String, String> foo = InfinispanUtils.getOrCreatePersistentMap("testmap");
        private static final long serialVersionUID = 2943370243108735555L;

        public MyReducer(Properties p) {
            conf = p;
        }

        @Override
        public String reduce(String s, Iterator<String> stringIterator) {
            if (!isInitialized) {
                isInitialized = true;
                index = 0;
            }
            StringBuilder builder = new StringBuilder();
            while (stringIterator.hasNext()) {
                String ss = stringIterator.next();
//                System.out.println("ss " + ss);
                builder.append(ss + ",");
            }

            foo.put(Integer.toString(index), s);
            index++;
            return conf.getProperty("suffix") + builder.toString();
        }
    }

    public static void main(String[] args) {
        InfinispanUtils.start();
        Cache c1 = (Cache) InfinispanUtils.getOrCreatePersistentMap("testData");
        Cache c2 = (Cache) InfinispanUtils.getOrCreatePersistentMap("testData2");

        c1.put("1", "Hello world here I am");
        c1.put("2", "blablablab la");
        c2.put("2", "InfinispanUtils rules the world");
        c1.put("3", "JUDCon is in Boston");
        c2.put("4", "JBoss World is in Boston as well");
        c1.put("12", "JBoss Application Server");
        c2.put("15", "Hello world");
        c1.put("14", "InfinispanUtils community");
        c2.put("15", "Hello world");

        c1.put("111", "InfinispanUtils open source");
        c2.put("112", "Boston is close to Toronto");
        c1.put("113", "Toronto is a capital of Ontario");
        c2.put("114", "JUDCon is cool");
        c1.put("211", "JBoss World is awesome");
        c2.put("212", "JBoss rules");
        c1.put("213", "JBoss division of RedHat ");
        c2.put("214", "RedHat community");

        MapReduceTask<String, String, String, String> t =
                new MapReduceTask<String, String, String, String>(c1, true, true);

        Properties conf = new Properties();
        conf.setProperty("prefix", "foo");
        conf.setProperty("suffix", "bar");
        t.mappedWith(new MyMapper(conf))
                .reducedWith(new MyReducer(conf));
        Map<String, String> wordCountMap = t.execute();
        for (Map.Entry<String, String> entry : wordCountMap.entrySet()) {
            System.out.println("k: " + entry.getKey() + " val " + entry.getValue() + "\n");
        }
        ConcurrentMap<String, String> foo = InfinispanUtils.getOrCreatePersistentMap("testmap");
        for (Map.Entry<String, String> entry : foo.entrySet()) {
            System.out.println("k: " + entry.getKey() + " val " + entry.getValue() + "\n");
        }
    }
}
