package eu.leads.processor.plan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.StdConverter;
import eu.leads.processor.execute.operators.*;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/5/13
 * Time: 4:36 PM
 * To change this template use File | Settings | File Templates.
 */

//Converter for ExecutionPlannode from JsonTreeNode
public class ExecutionPlanNodeConverter extends StdConverter<JsonNode, ExecutionPlanNode> {
    @Override
    public ExecutionPlanNode convert(JsonNode jsonNode) {
        ObjectMapper mapper = new ObjectMapper();
        OperatorType t = OperatorType.valueOf(jsonNode.path("type").asText());
        ExecutionPlanNode result = null;
        try {
            switch (t) {
                case READ:
                    result = mapper.readValue(jsonNode.toString(), ReadOperator.class);
                    break;
                case PROJECT:
                    result = mapper.readValue(jsonNode.toString(), ProjectOperator.class);
                    break;
                case RENAME:
                    break;
                case JOIN:
                    break;
                case GROUPBY:
                    result = mapper.readValue(jsonNode.toString(), GroupByOperator.class);
                    break;
                case SORT:
                    result = mapper.readValue(jsonNode.toString(), SortOperator.class);
                    break;
                case DISTINCT:
                    break;
                case FILTER:
                    result = mapper.readValue(jsonNode.toString(), FilterOperator.class);
                    break;
                case LIMIT:
                    result = mapper.readValue(jsonNode.toString(), LimitOperator.class);
                    break;
                case OUTPUT:
                    result = mapper.readValue(jsonNode.toString(), OutputOperator.class);
                    break;
                case NONE:
                    break;
            }
        } catch (IOException e) {
            System.out.println("json " + jsonNode.toString());
            e.printStackTrace();
        }
        return result;
    }

}
