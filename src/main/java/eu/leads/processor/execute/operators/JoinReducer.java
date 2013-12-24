package eu.leads.processor.execute.operators;

import eu.leads.processor.execute.LeadsReducer;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.utils.InfinispanUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/7/13
 * Time: 8:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class JoinReducer extends LeadsReducer<String, String> {


    String prefix;

    @Override
    public void initialize() {
        isInitialized = true;
        super.initialize();
        prefix = conf.getProperty("output") + ":";
        output = InfinispanUtils.getOrCreatePersistentMap(prefix);
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        RandomAccessFile raf = null;
        try {
            String filename = System.getenv().get("HOME") + "/queryProcessor/" + hostname;
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

    public JoinReducer(Properties configuration) {
        super(configuration);
    }

    @Override
    public String reduce(String key, Iterator<String> iterator) {
        if (!isInitialized)
            initialize();
        ArrayList<Tuple> left = new ArrayList<Tuple>();
        ArrayList<Tuple> right = new ArrayList<Tuple>();

        String leftTable = conf.getProperty("left");
//        String rightTable = conf.getProperty("right");
        ArrayList<String> ignoreColumns = new ArrayList<String>();
        ignoreColumns.add("table");
        ignoreColumns.add("tupleId");
//        ignoreColumns.add((String) conf.getProperty(rightTable));
        while (iterator.hasNext()) {
            String tstring = iterator.next();
            Tuple t = new Tuple(tstring);
            if (t.getAttribute("table").equals(leftTable)) {
                left.add(t);
            } else {
                right.add(t);
            tstring = null;
            }
        }

        for (Tuple tl : left) {
            for (Tuple tr : right) {
                Tuple resultTuple = new Tuple(tl, tr, ignoreColumns);
//                System.out.println(this.getClass().toString()+" proc tuple");
                output.put(prefix + tr.getAttribute("tupleId") + "-" + tl.getAttribute("tupleId"), resultTuple.asString());
                progress();
                resultTuple = null;
            }
        }
        left.clear();
        right.clear();
        left = null;
        right = null;
        ignoreColumns.clear();
        ignoreColumns = null;
        return "";
    }

}
