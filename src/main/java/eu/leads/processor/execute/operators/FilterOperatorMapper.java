package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.LeadsMapper;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.math.MathOperatorTree;
import org.infinispan.distexec.mapreduce.Collector;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 1:39 PM
 * To change this template use File | Settings | File Templates.
 */
//Filter Mapper
public class FilterOperatorMapper extends LeadsMapper<String, String, String, String> {
    String prefix;
    transient MathOperatorTree tree;
    String queryId;
    ConcurrentMap<String, String> queries;
    ConcurrentMap<String, String> datamap;
    transient QueryContext context;

    public FilterOperatorMapper(Properties configuration) {
        super(configuration);

    }

    public void initialize() {
        super.initialize();
        isInitialized = true;
        queryId = conf.getProperty("queryId");
        prefix = conf.getProperty("output") + ":";
        ObjectMapper mapper = new ObjectMapper();
        try {
            tree = mapper.readValue(conf.getProperty("tree"), MathOperatorTree.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        queries = InfinispanUtils.getOrCreatePersistentMap("queries");
        datamap = InfinispanUtils.getOrCreatePersistentMap(prefix);
        String query = queries.get(queryId);
        try {
            JsonNode root = mapper.readTree(query);
            root = root.path("context");
            context = mapper.readValue(root.toString(), QueryContext.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        RandomAccessFile raf = null;
        try {
            String filename = "/tmp/queryProcessor." + hostname;
            File f = new File(filename);
            long fileLength = f.length();
            raf = new RandomAccessFile(filename, "rw");
            raf.seek(fileLength);

            raf.writeBytes("Running " + hostname + ": " + this.getClass().getCanonicalName() + "\n");
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
        Tuple tuple = new Tuple(value);
        progress();
        String tupleId = key.substring(key.indexOf(":") + 1);
        if (tree.accept(tuple, context)) {
            datamap.put(prefix + tupleId, tuple.asString());
        }
    }
}
