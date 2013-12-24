package eu.leads.processor.execute;

import eu.leads.processor.utils.StdOutputWriter;
import org.infinispan.distexec.mapreduce.Reducer;

import java.util.Iterator;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentMap;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 6:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class LeadsReducer<kOut, vOut> implements Reducer<kOut, vOut> {
    protected final Properties conf;
    protected ConcurrentMap<String, String> output;
    protected boolean isInitialized = false;

    protected long overall;
    transient protected Timer timer;
    protected ProgressReport report;

    public LeadsReducer(Properties configuration) {
        this.conf = configuration;

    }

    public void initialize() {
        overall = Long.parseLong(this.conf.getProperty("workload"));
        timer = new Timer();
        report = new ProgressReport(this.getClass().toString(), 0, overall);
        timer.scheduleAtFixedRate(report, 0, 2000);

    }

    @Override
    public vOut reduce(kOut kOut, Iterator<vOut> iterator) {
        vOut firstValue = iterator.next();
        return firstValue;
    }

    @Override
    protected void finalize() {
        report.printReport(report.getReport());
        StdOutputWriter.getInstance().println("");
        timer.cancel();
    }

    protected void progress() {
        report.tick();
    }

    protected void progress(long n) {
        report.tick(n);
    }
}
