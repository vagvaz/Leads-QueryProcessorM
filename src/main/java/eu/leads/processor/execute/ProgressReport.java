package eu.leads.processor.execute;

import eu.leads.processor.utils.StdOutputWriter;

import java.io.*;
import java.util.TimerTask;

/**
 * Created by vagvaz on 12/17/13.
 */
public class ProgressReport extends TimerTask implements Serializable {

    private long ticks;
    private long overall;
    private boolean maxed;
    private String prefix;

    public ProgressReport(String prefix, long ticks, long overall) {
        this.ticks = ticks;
        this.overall = overall;
        maxed = false;
        this.prefix = prefix;
    }

    @Override
    public void run() {
        if (maxed) {
            this.cancel();
            StdOutputWriter.getInstance().println("");
            return;
        }
        double report = getReport();
        printReport(report);
//        try {

//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

    public void printReport(double report) {
        if (report >= 0.999999998)
            maxed = true;
        String tmp = "";
        if (!maxed)
//            tmp  = prefix + " processed: " + Integer.toString((int)(Math.ceil(report*100))) + "%\r";
            tmp = prefix + " processed: " + Long.toString(ticks) + " tuples\r";
        else
//            tmp  = prefix + " processed: " + Integer.toString((int)(Math.ceil(report*100))) + "%\n";
            tmp = prefix + " processed: " + Long.toString(ticks) + " tuples\n";
        StdOutputWriter.getInstance().write(tmp);
    }

    public void tick() {
        ticks++;
    }

    public void tick(long t) {
        ticks += t;
    }

    public double getReport() {
        if (overall > 0)
            return Math.min(1.0, (double) ticks / overall);
        else
            return 0.0;
    }

    private void writeObject(ObjectOutputStream out) {

        try {
            out.writeLong(ticks);
            out.writeLong(overall);
            out.writeBoolean(maxed);
            out.writeUTF(prefix);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readObject(ObjectInputStream in) {
        try {
            ticks = in.readLong();
            overall = in.readLong();
            maxed = in.readBoolean();
            prefix = in.readUTF();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readObjectNoData() {
        ticks = 0;
        overall = 0;
        maxed = false;
    }
}
