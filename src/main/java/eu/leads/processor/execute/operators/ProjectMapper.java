package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.LeadsMapper;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.utils.InfinispanUtils;
import net.sf.jsqlparser.schema.Column;
import org.infinispan.Cache;
import org.infinispan.distexec.mapreduce.Collector;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.System.getProperties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 8:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class ProjectMapper extends LeadsMapper<String, String, String, String> {
    private Cache<String, String> output = null;
    private String prefix = "";
    private List<String> columns;

    public ProjectMapper(Properties configuration) {
        super(configuration);
    }

    @Override
    public void initialize() {
        isInitialized = true;
        super.initialize();
        prefix = conf.getProperty("output") + ":";
        output = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap(prefix);
        ObjectMapper mapper = new ObjectMapper();
        List<Column> c = null;
        try {
            c = mapper.readValue(conf.getProperty("columns"), new TypeReference<List<Column>>() {
            });
            columns = new ArrayList<String>();
            for (Column col : c) {
                columns.add(col.getColumnName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

//        String hostname = "";
//        try {
//            hostname = InetAddress.getLocalHost().getHostName();
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
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

        progress();
        String tupleId = key.substring(key.indexOf(':') + 1);
        Tuple projected = new Tuple(value);

        handlePagerank(projected);
        projected.keepOnly(columns);

        output.put(prefix + tupleId, projected.asString());
//        System.err.println(this.getClass().toString()+" proc tuple " + prefix+tupleId + "t: "+projected.asString() + " " + output.size());
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
////                    d = LeadsPrGraph.getPageDistr(t.getAttribute("url"));
//                    d = (double) LeadsPrGraph.getPageVisitCount(t.getAttribute("url"));
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                t.setAttribute("pagerank", d.toString());
//        }
    }
}


}
