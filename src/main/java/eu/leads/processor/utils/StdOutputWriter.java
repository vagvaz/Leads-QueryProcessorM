package eu.leads.processor.utils;

import eu.leads.processor.conf.WP3Configuration;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by vagvaz on 12/17/13.
 */
//This class is used in order to give feedback to the user
public class StdOutputWriter implements Runnable {

    private static StdOutputWriter singleton;
    private static ConcurrentLinkedQueue<String> queue;
    private static final Object mutex = new Object();
    private static Thread threadWriter;
    private static boolean isRunning = false;

    private StdOutputWriter() {

    }

    public static StdOutputWriter getInstance() {
        if (singleton == null) {
            synchronized (StdOutputWriter.class) {
                singleton = new StdOutputWriter();
                threadWriter = new Thread(singleton);
                isRunning = true;
                threadWriter.start();
                queue = new ConcurrentLinkedQueue<String>();
            }

        }
        return singleton;
    }

    @Override
    public void run() {
        while (isRunning) {

            if (queue.isEmpty()) {
                waitForInput();
            }
            while (!queue.isEmpty()) {
                String out = queue.poll();
                try {
                    System.out.write(out.getBytes());
                    out = null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.flush();
            }
        }
    }

    private static void waitForInput() {
        synchronized (mutex) {
            try {
                mutex.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void println(String out) {
        if(!WP3Configuration.getBoolean("loginfo"))
            return;
        String newOut = out + "\n";
        queue.add(newOut);
        notifyForInput();

    }

    public void printlnAndClear(String out) {
        String newOut = out + "\n";
        System.out.flush();
        queue.clear();
        System.out.flush();
        queue.add(newOut);
        notifyForInput();

    }

    public void write(String out) {
//        if(!WP3Configuration.getBoolean("loginfo"))
//            return;
        queue.add(out);
        notifyForInput();
    }

    private static void notifyForInput() {
        synchronized (mutex) {
            mutex.notify();
        }
    }

    public static void stop() {
        notifyForInput();
        isRunning = false;
    }
}
