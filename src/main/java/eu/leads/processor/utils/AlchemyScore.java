package eu.leads.processor.utils;

import com.alchemyapi.api.AlchemyAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;

import static java.lang.System.getProperties;

/**
 * @author John
 */
public class AlchemyScore {
    private static AlchemyAPI alchemyObj = null;
    public static void initialize(){
        try {
            alchemyObj = AlchemyAPI.GetInstanceFromFile(getProperties().getProperty("processorSentimentAnalysisKeyFile"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static float getScore(String url, String term) {
        float score = -2;
        try {
            Document doc = alchemyObj.URLGetTargetedSentiment(url, term);
            doc.getDocumentElement().normalize();
            NodeList nList = doc.getElementsByTagName("results");
            Node nNode = nList.item(0);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                if (eElement.getElementsByTagName("docSentiment").item(0).getTextContent().trim().contains("neutral")) {
                    score = 0;
                } else {
                    String score2 = eElement.getElementsByTagName("docSentiment").item(0).getTextContent().trim().split("\n")[1];
                    score = Float.parseFloat(score2);
                }
            }
        } catch (RuntimeException re) {
            re.printStackTrace();
        } catch (Exception e) {
//            System.out.println(e.getMessage());
            return -2;
        }
        return score;
    }

    public static double getScore(String url) {

        try {
            double score = 0.0;
            Document doc = alchemyObj.URLGetTextSentiment(url);
            //System.out.println(getStringFromDocument(doc));
            doc.getDocumentElement().normalize();
            NodeList nList = doc.getElementsByTagName("results");
            Node nNode = nList.item(0);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                if (eElement.getElementsByTagName("docSentiment").item(0).getTextContent().trim().contains("neutral")) {
                    score = 0;
                } else {
                    String score2 = eElement.getElementsByTagName("docSentiment").item(0).getTextContent().trim().split("\n")[1];
                    score = Float.parseFloat(score2);
                }
            }
            return (float) score;
        } catch (IOException ex) {
            //System.out.println(ex.getMessage());
            if (ex.getMessage().contains("daily-transaction-limit-exceeded")) {
                changeFile();
            }
            return -2;
        } catch (SAXException saex) {
            return -2;
        } catch (ParserConfigurationException pcex) {
            return -2;
        } catch (XPathExpressionException xpathex) {
            return -2;
        } catch (IllegalArgumentException ilex) {
            return -2;
        }
    }

    private static void changeFile() {


    }
}
