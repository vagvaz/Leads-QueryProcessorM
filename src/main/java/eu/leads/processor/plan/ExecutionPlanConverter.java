package eu.leads.processor.plan;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.StdConverter;
import eu.leads.processor.execute.operators.OutputOperator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/5/13
 * Time: 4:11 PM
 * To change this template use File | Settings | File Templates.
 */
//
public class ExecutionPlanConverter extends StdConverter<JsonNode, ExecutionPlan> {
    @Override
    public ExecutionPlan convert(JsonNode jsonNode) {
        ObjectMapper mapper = new ObjectMapper();
        try {
//            OutputOperator output = mapper.readValue(jsonNode.path("output").toString(), OutputOperator.class);
            Map<String, ExecutionPlanNode> graph = mapper.readValue(jsonNode.path("graph").toString(), new TypeReference<Map<String, ExecutionPlanNode>>() {
            });
            List<String> sources = mapper.readValue(jsonNode.path("sources").toString(), new TypeReference<List<String>>() {
            });
            return new ExecutionPlan(new OutputOperator("nana"), graph, sources);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ExecutionPlan();
    }
}
