package eu.leads.processor;

import eu.leads.processor.conf.WP3Configuration;
import eu.leads.processor.utils.CommandLineUtil;
import eu.leads.processor.utils.InfinispanUtils;
import eu.leads.processor.utils.StdOutputWriter;
import eu.leads.processor.utils.listeners.AttributeListener;
import eu.leads.processor.utils.listeners.TrackListener;
import org.infinispan.Cache;

/**
 * Created by vagvaz on 12/23/13.
 */
public class TrackService {
    public static void main(String[] args) {

        WP3Configuration.initialize();
        //Start infinispan
        InfinispanUtils.start();
        String crawlerCache = WP3Configuration.getProperty("crawlerCache");
        //Override from command line argument
        if (args.length > 0)
            crawlerCache = args[0];
        Cache<String, String> crCache = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap(crawlerCache);
        TrackListener listener = new TrackListener("webpages:");
        crCache.addListener(listener);
        System.out.println("Initalize listener for webpages");
        AttributeListener attributeListener = new AttributeListener("webpages", "body", "adidas", "entities");
        InfinispanUtils.addListenerForMap(attributeListener, "webpages:");
        System.out.println("Initialize listener for webpages.body like \'adidas\'");

        CommandLineUtil console = new CommandLineUtil(System.in,System.out);
        StringBuilder input = new StringBuilder();
        Cache<String, String> control = (Cache<String, String>) InfinispanUtils.getOrCreatePersistentMap("processorControl");
        control.put("service","active");

        String line = console.read("Write quit; to stop the service.");

        while(true){
            line = line.trim();
            input.append(line);

            while (!line.trim().endsWith(";") && !input.toString().endsWith(";")) {
                line = console.readLine();
                input.append(" " + line.trim());
            }


            StdOutputWriter.getInstance().println("");
            String query = input.toString();

            //Quit the SQLInterface
            if (query.toLowerCase().trim().equals("quit;")) {
               break;
            }
            System.out.println(line);
            line = console.read("Write quit; to stop the service.");
            input = new StringBuilder();
        }
        StdOutputWriter.getInstance().printlnAndClear("The Track Service has been stopped.");
        control.remove("service");
        System.exit(0);
    }

}
