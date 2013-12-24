package eu.leads.processor.utils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import eu.leads.crawler.model.Page;
import org.apache.http.impl.cookie.DateParseException;

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/7/13
 * Time: 2:44 PM
 * To change this template use File | Settings | File Templates.
 */
//Class that is used to store webpages that can be processed.
@JsonAutoDetect
public class LeadsProcessorPage {

    private String domainName;
    private Date published;
    private List<String> links;
    private String url;
    private String body;
    private Double sentiment;
    private Double pagerank;

    @JsonCreator
    public LeadsProcessorPage(@JsonProperty("url") String url, @JsonProperty("body") String body, @JsonProperty("sentiment") Double sentiment, @JsonProperty("pagerank") Double pagerank, @JsonProperty("domainName") String domainName, @JsonProperty("published") Date publicationDate, @JsonProperty List<String> links) {
        setUrl(url);
        setBody(body);
        setSentiment(sentiment);
        setPagerank(pagerank);
        setDomainName(domainName);
        setPublished(publicationDate);
        setLinks(links);
    }


    public LeadsProcessorPage(Page crawlerPage) {
        domainName = crawlerPage.getDomainName();
        try {
            String pubString = crawlerPage.getHeaders().get("last-modified");
            if (pubString != null) {
                published = org.apache.http.impl.cookie.DateUtils.parseDate(pubString);

            } else {
                published = null;
            }
            links = new ArrayList<String>(crawlerPage.getLinks().size());

            for (URL link : crawlerPage.getLinks()) {
                links.add(link.toString());
            }
            url = crawlerPage.getUrl().toString();
            body = crawlerPage.getContent();
//            body = "";
        } catch (DateParseException e) {


        }

    }

    public String getDomainName() {
        return domainName;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }

    public Date getPublished() {
        return published;
    }

    public void setPublished(Date published) {
        this.published.setTime(published.getTime());
    }

    public List<String> getLinks() {
        return links;
    }

    public void setLinks(List<String> links) {
        this.links = links;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public Double getSentiment() {
        return sentiment;
    }

    public void setSentiment(Double sentiment) {
        this.sentiment = sentiment;
    }

    public Double getPagerank() {
        return pagerank;
    }

    public void setPagerank(Double pagerank) {
        this.pagerank = pagerank;
    }

}
