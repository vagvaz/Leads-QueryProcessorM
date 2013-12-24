package eu.leads.processor.utils.math;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.StdConverter;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/3/13
 * Time: 10:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class MathTreeOperatorNodeConverter extends StdConverter<JsonNode, MathOperatorTreeNode> {
    @Override
    public MathOperatorTreeNode convert(JsonNode jsonNode) {
        MathOperatorTreeNode left;
        MathOperatorTreeNode right;

        if (jsonNode.path("type").asText().equals("EXPRESSION")) {
            MathTreeOperatorNodeConverter converter = new MathTreeOperatorNodeConverter();
            left = converter.convert(jsonNode.path("left"));
            right = converter.convert(jsonNode.path("right"));
            return new MathOperatorTreeNode(left, right, jsonNode.path("not").asBoolean(), jsonNode.path("expressionFunction").asText());
        } else {
//            ObjectMapper mapper = new ObjectMapper();
            return new MathOperatorTreeNode(jsonNode.path("operand").asText());
        }
    }
}
