package eu.leads.processor.execute.operators;

import eu.leads.processor.execute.LeadsMapper;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.utils.InfinispanUtils;
import org.infinispan.distexec.mapreduce.Collector;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.Properties;

import static java.lang.System.getProperties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/7/13
 * Time: 8:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class JoinMapper extends LeadsMapper<String, String, String, String> {

    public JoinMapper(Properties configuration) {
        super(configuration);
    }

    public void initialize() {
        isInitialized = true;
        super.initialize();
        RandomAccessFile raf = null;
        try {
            String filename = getProperties().getProperty("java.io.tmpdir") + "/queryProcessor." + InfinispanUtils.getMemberName();
            File f = new File(filename);
            long fileLength = f.length();
            raf = new RandomAccessFile(filename, "rw");
            raf.seek(fileLength);

            raf.writeBytes("Running " + InfinispanUtils.getMemberName() + ": " + this.getClass().getCanonicalName() + "\n");
            raf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(String key, String value, Collector<String, String> collector) {
        if (!isInitialized)
            initialize();
        String tableName = key;
        String columnName = (String) conf.getProperty(tableName);

        Map<String, String> tuples = InfinispanUtils.getOrCreatePersistentMap(key);
        String outkey = "";
        for (Map.Entry<String, String> entry : tuples.entrySet()) {
            progress();
            Tuple t = new Tuple(entry.getValue());
            handlePagerank(t);
            outkey = t.getAttribute(columnName);
            t.setAttribute("tupleId", entry.getKey().substring(entry.getKey().indexOf(":") + 1));
            t.setAttribute("table", tableName);
            collector.emit(outkey, t.asString());
            t = null;
            outkey = null;
        }
    }

    private void handlePagerank(Tuple t) {
        if (t.hasField("pagerank")) {
            if (!t.hasField("url"))
                return;
            String pagerankStr = t.getAttribute("pagerank");
//            Double d = Double.parseDouble(pagerankStr);
//            if (d < 0.0) {
//
//                try {
//                    d = (double) LeadsPrGraph.getPageVisitCount(t.getAttribute("url"));
//                    System.out.println("vs cnt " + LeadsPrGraph.getTotalVisitCount());
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                t.setAttribute("pagerank", d.toString());
//            }
        }
    }

}
