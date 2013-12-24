package eu.leads.processor.conf;

import eu.leads.processor.utils.StringConstants;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import static java.lang.System.getProperties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/22/13
 * Time: 2:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class WP3Configuration {

    static final String clusterName = "";
    static final String nodeName = "";


    static Properties properties;

    public static void initialize() {

        properties = getProperties();
        if (!loadPropertiesFile("processor.properties"))
            loadDefaultWP3Values();
        if (!loadPropertiesFile("config.properties"))
            loadDefaultCrawlerValues();
        if (!properties.containsKey(StringConstants.CLUSTER_NAME_KEY)) {
            properties.setProperty(StringConstants.CLUSTER_NAME_KEY, StringConstants.DEFAULT_CLUSTER_NAME);
        }
        if (!properties.containsKey(StringConstants.NODE_NAME_KEY)) {
            properties.setProperty(StringConstants.NODE_NAME_KEY, StringConstants.DEFAULT_NODE_NAME);
        }
        if(!properties.containsKey("crawlerCache"))
            properties.setProperty("crawlerCache", "preprocessingMap");
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        properties.setProperty("hostname", hostname);


    }

    private static void loadDefaultCrawlerValues() {
        properties.setProperty("crawlerCache", "preprocessingMap");
    }

    private static void loadDefaultWP3Values() {
        properties.setProperty(StringConstants.CLUSTER_NAME_KEY, StringConstants.DEFAULT_CLUSTER_NAME);
        properties.setProperty(StringConstants.NODE_NAME_KEY, StringConstants.DEFAULT_NODE_NAME);

    }

    private static boolean loadPropertiesFile(String filename) {
        boolean result = true;
        try {
            properties.load(WP3Configuration.class.getClassLoader().getResourceAsStream(filename));
            System.out.println("Found " + filename);
        } catch (Exception e) {
            System.out.println("Could not found " + filename);
            result = false;
        }
        return result;
    }

    public static String getMicroClusterName() {
        return properties.getProperty(clusterName);
    }

    public static String getNodeName() {
        return properties.getProperty(nodeName);
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static boolean getBoolean(String key) {
        if (properties.containsKey(key))
            return Boolean.parseBoolean(properties.getProperty(key));
        return false;
    }

    public static Integer getInt(String key) {
        if (properties.containsKey(key))
            return Integer.parseInt(properties.getProperty(key));
        return 0;
    }

    public static Double getDouble(String key) {
        if (properties.containsKey(key)) {
            return Double.parseDouble(properties.getProperty(key));
        }
        return 0.0;
    }

    public static String getHostname() {
        return properties.getProperty("hostname");
    }
}
