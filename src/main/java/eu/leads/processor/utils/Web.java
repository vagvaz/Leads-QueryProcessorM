package eu.leads.processor.utils;

import eu.leads.crawler.utils.JenkinsHash;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

/**
 * @author P. Sutra
 */
public class Web {

    /**
     * @param domain - (String)
     * @return PR rating (int) or -1 if unavailable or internal error happened.
     */
    public static int pagerank(String domain) {

        String result = "";

        JenkinsHash jenkinsHash = new JenkinsHash();
        long hash = jenkinsHash.hash(("info:" + domain).getBytes());
        String url = "http://toolbarqueries.google.com/tbr?client=navclient-auto&hl=en&"
                + "ch=6" + hash + "&ie=UTF-8&oe=UTF-8&features=Rank&q=info:" + domain;

        try {
            URLConnection conn = new URL(url).openConnection();

            BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));

            String input;
            while ((input = br.readLine()) != null) {
                result = input.substring(input.lastIndexOf(":") + 1);
            }
            br.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        if ("".equals(result)) {
            return 0;
        } else {
            return Integer.valueOf(result);
        }

    }

}