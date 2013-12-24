package eu.leads.processor.utils;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/8/13
 * Time: 10:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class CommandLineUtil {
    private final BufferedReader reader;
    private final Writer writer;
    public CommandLineUtil() {
        reader = new BufferedReader(new InputStreamReader(System.in));
        writer = new BufferedWriter(new OutputStreamWriter(System.out));
    }

    public CommandLineUtil(InputStream is, OutputStream os) {
        reader = new BufferedReader(new InputStreamReader((is)));
        writer = new OutputStreamWriter(os);
    }

    public void show(String line) {
        try {
            StdOutputWriter.getInstance().write(line + "\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public String readLine() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return "";
    }

    public String read(String message) {
        try {
            show(message);
            String result = reader.readLine();

            return result;
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return "";
    }

    public void info(String s) {
        show(s);
    }

    public void error(String s) {
        show(s);
    }
}
