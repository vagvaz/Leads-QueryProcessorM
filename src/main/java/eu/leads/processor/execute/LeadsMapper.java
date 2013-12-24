package eu.leads.processor.execute;

import eu.leads.processor.utils.StdOutputWriter;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.Mapper;

import java.util.Properties;
import java.util.Timer;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 5:58 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class LeadsMapper<kIN, vIN, kOut, vOut> implements Mapper<kIN, vIN, kOut, vOut> {
    protected final Properties conf;
    protected boolean isInitialized = false;

    //    protected Cache<String,String> cache;
    protected long overall;
    protected Timer timer;
    protected ProgressReport report;

    public LeadsMapper(Properties configuration) {
        this.conf = configuration;
    }

    public void initialize() {
        overall = Long.parseLong(this.conf.getProperty("workload"));
        timer = new Timer();
        report = new ProgressReport(this.getClass().toString(), 0, overall);
        timer.scheduleAtFixedRate(report, 0, 2000);

    }

    @Override
    public void map(kIN key, vIN value, Collector<kOut, vOut> collector) {
//        kOut outkey = (kOut) key;
//        vOut outvalue = (vOut) value;
//        collector.emit(outkey, outvalue);
    }

    @Override
    protected void finalize() {
        report.printReport(report.getReport());
        StdOutputWriter.getInstance().println("");
        report.cancel();
        timer.cancel();
    }

    protected void progress() {
        report.tick();
    }

    protected void progress(long n) {
        report.tick(n);
    }

    protected double getProgress() {
        return report.getReport();
    }

}
