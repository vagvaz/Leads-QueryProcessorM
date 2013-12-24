package eu.leads.processor.plan;

import com.fasterxml.jackson.databind.JsonNode;

import javax.enterprise.inject.Produces;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/21/13
 * Time: 1:17 PM
 * To change this template use File | Settings | File Templates.
 */
//factory to return Basic Plan Extractor
public class ExtractorFactory {

    public static
    @Produces
    BasicPlannerExtractor getBasicExtractor(StatementType type, JsonNode node) {
        switch (type) {
            case SELECT:
                return new SelectExtractor(node);
            case INSERT:
                return null;
            case DELETE:
                return null;
            case CREATETABLE:
                return null;
            case UPDATE:
                return null;
            default:
                return null;
        }
    }
}

