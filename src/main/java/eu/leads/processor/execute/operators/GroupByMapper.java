package eu.leads.processor.execute.operators;

import eu.leads.processor.execute.LeadsMapper;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.utils.InfinispanUtils;
import org.infinispan.distexec.mapreduce.Collector;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import static java.lang.System.getProperties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/3/13
 * Time: 4:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class GroupByMapper extends LeadsMapper<String, String, String, String> {

    final List<String> columns;

    public GroupByMapper(Properties configuration) {
        super(configuration);
        columns = new ArrayList<String>();
    }


    @Override
    public void map(String key, String value, Collector<String, String> collector) {
        if (!isInitialized)
            intialize();
        StringBuilder builder = new StringBuilder();
//        String tupleId = key.substring(key.indexOf(":"));
        Tuple t = new Tuple(value);
        progress();
        for (String c : columns) {
            builder.append(t.getAttribute(c) + ",");
        }
//        t.setAttribute("LEADSTUPLEID",tupleId);
        collector.emit(builder.toString(), t.asString());
    }

    private void intialize() {
        isInitialized = true;
        super.initialize();
        StringTokenizer tokenizer = new StringTokenizer(conf.getProperty("columns").trim(), ",");
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            columns.add(token);
        }


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
}
