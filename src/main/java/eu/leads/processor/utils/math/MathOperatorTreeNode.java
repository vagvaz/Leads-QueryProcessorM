package eu.leads.processor.utils.math;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.utils.SQLUtils;
import net.sf.jsqlparser.schema.Column;

import java.io.IOException;

import static eu.leads.processor.utils.math.MathOperatorTreeNode.NodeType.EXPRESSION;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 5:17 AM
 * To change this template use File | Settings | File Templates.
 */

//Basic Class needed to create the tree node.
@JsonAutoDetect
@JsonDeserialize(converter = MathTreeOperatorNodeConverter.class)
public class MathOperatorTreeNode {


    public enum NodeType {EXPRESSION, OPERAND}


    private MathOperatorTreeNode left;
    private MathOperatorTreeNode right;
    private String operand;
    private String expressionFunction;
    private boolean not;
    private NodeType type;
    private String valueType;
    private String value;

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public MathOperatorTreeNode(JsonNode jsonNode) {
        if (jsonNode.has("stringExpression") || jsonNode.has("expression")) {
            type = EXPRESSION;
            expressionFunction = jsonNode.path("stringExpression").asText();
            not = jsonNode.path("not").asBoolean();
            JsonNode node = null;
            if (!jsonNode.has("leftExpression"))
                node = jsonNode.path("expression");
            else
                node = jsonNode;
            left = new MathOperatorTreeNode(node.path("leftExpression"));
            right = new MathOperatorTreeNode((node.path("rightExpression")));
            operand = null;
        } else {
            type = NodeType.OPERAND;
            left = null;
            right = null;
            expressionFunction = "";
            ObjectMapper mapper = new ObjectMapper();
            try {
                operand = mapper.writeValueAsString(jsonNode);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }

    public MathOperatorTreeNode(String operand) {
        type = NodeType.OPERAND;
        left = null;
        right = null;
        expressionFunction = "";
        setOperand(operand);
        setNot(false);
    }


    public MathOperatorTreeNode(MathOperatorTreeNode left, MathOperatorTreeNode right, boolean isNot, String expressionFunction) {
        setLeft(left);
        setRight(right);
        setNot(isNot);
        setExpressionFunction(expressionFunction);
        operand = null;
        type = EXPRESSION;

    }

    @JsonIgnore
    public boolean accept(Tuple tuple, QueryContext context) {
        if (type == NodeType.EXPRESSION) {
            if (left.accept(tuple, context) && right.accept(tuple, context)) {
//                String leftValue = left.getValue();
//                String rightValue = right.getValue();
                return calculate(left, right, expressionFunction);
            }
            return false;
        } else {
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode jsonNode = mapper.readTree(operand);
                if (jsonNode.has("columnName")) {
                    Column c = mapper.readValue(jsonNode.toString(), Column.class);
                    String columnType = context.getColumnType(c.getColumnName(), context.resolveTableName(c.getColumnName()));
                    valueType = "column." + c.getColumnName() + "." + columnType;
                    value = tuple.getAttribute(c.getColumnName());
                } else if (jsonNode.has("value")) {
                    if (jsonNode.has("stringValue")) {
                        value = jsonNode.path("stringValue").asText();
                        valueType = ".string";
                    } else {
                        value = jsonNode.path("value").asText();
                        valueType = ".constant";
                    }
                } else if (jsonNode.has("name")) {
                    String c = SQLUtils.extractFunction(jsonNode).toString();
                    valueType = "column." + c + ".double";
                    value = tuple.getAttribute(c.toString());

                } else {
                    valueType = "null";
                    value = "NULL";
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return true;
        }
    }

    private boolean calculate(MathOperatorTreeNode left, MathOperatorTreeNode right, String expression) {
        boolean result = true;
        valueType = "intermediate.boolean";
        if (expression.equals("LIKE")) {
            result = left.getValue().toLowerCase().contains(right.getValue().toLowerCase());
        } else if (expression.equals("<")) {
            if (left.getValueType().startsWith("column")) {
                String leftType = left.getValueType();
                String c = leftType.substring(leftType.lastIndexOf('.') + 1);
                if (c.equalsIgnoreCase("double") || c.equalsIgnoreCase("long") || c.equalsIgnoreCase("int")) {
                    Double l = Double.valueOf(left.getValue());
                    Double r = Double.valueOf(right.getValue());
                    result = l < r;
                } else if (c.equals("string")) {
                    result = left.getValue().compareTo(right.getValue()) < 0;
                } else if (c.equals("date")) {
                    //TODO
                    Long l = Long.valueOf(left.getValue());
                    Long r = Long.valueOf(right.getValue());
                    result = l < r;
                } else if (c.equals("NULL")) {
                    result = true;
                } else if (c.equals("constant")) {
                    String rightType = right.getValueType();
                    String r = rightType.substring(rightType.lastIndexOf('.') + 1);
                    if (!r.equals("constant")) {
                        result = calculate(right, left, ">");
                    } else
                        result = left.getValue().compareTo(right.getValue()) < 0;
                }

            }
        } else if (expression.equals(">")) {
            String leftType = left.getValueType();
            String c = leftType.substring(leftType.lastIndexOf('.') + 1);
            if (c.equalsIgnoreCase("double") || c.equalsIgnoreCase("long") || c.equalsIgnoreCase("int")) {
                Double l = Double.valueOf(left.getValue());
                Double r = Double.valueOf(right.getValue());
                result = l > r;
            } else if (c.equals("string")) {
                result = left.getValue().compareTo(right.getValue()) > 0;
            } else if (c.equals("date")) {
                //TODO
                Long l = Long.valueOf(left.getValue());
                Long r = Long.valueOf(right.getValue());
                result = l > r;
            } else if (c.equals("NULL")) {
                result = true;
            } else if (c.equals("constant")) {
                String rightType = right.getValueType();
                String r = rightType.substring(rightType.lastIndexOf('.') + 1);
                if (!r.equals("constant")) {
                    result = calculate(right, left, "<");
                } else
                    result = left.getValue().compareTo(right.getValue()) > 0;
            }

        } else if (expression.equalsIgnoreCase("OR")) {
            if (left.getValueType().endsWith("boolean") && right.getValueType().endsWith("boolean")) {
                result = Boolean.valueOf(left.getValue()) || Boolean.valueOf(right.getValue());
            }
            result = true;

        } else if ((expression.equalsIgnoreCase("AND"))) {
            if (left.getValueType().endsWith("boolean") && right.getValueType().endsWith("boolean")) {
                result = Boolean.valueOf(left.getValue()) && Boolean.valueOf(right.getValue());
            }
            result = true;
        }
        value = Boolean.toString(result);
        return result;
    }


    public MathOperatorTreeNode getLeft() {
        return left;
    }

    public void setLeft(MathOperatorTreeNode left) {
        this.left = left;
    }

    public MathOperatorTreeNode getRight() {
        return right;
    }

    public void setRight(MathOperatorTreeNode right) {
        this.right = right;
    }

    public String getOperand() {
        return operand;
    }

    public void setOperand(String operand) {
        this.operand = operand;
    }

    public String getExpressionFunction() {
        return expressionFunction;
    }

    public void setExpressionFunction(String expressionFunction) {
        this.expressionFunction = expressionFunction;
    }

    public boolean isNot() {
        return not;
    }

    public void setNot(boolean not) {
        this.not = not;
    }

    public NodeType getType() {
        return type;
    }

    public void setType(NodeType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        String result = "";
        if (type == NodeType.OPERAND && operand != null) {
//            result += "\ntype: operand " + operand.toString();
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode jsonNode = mapper.readTree(operand);
                if (jsonNode.has("columnName")) {
                    Column c = mapper.readValue(jsonNode.toString(), Column.class);
                    result += c.toString();
                } else if (jsonNode.has("value")) {
                    if (jsonNode.has("stringValue")) {
                        result += jsonNode.path("stringValue").asText();

                    } else {
                        result += jsonNode.path("value").asText();
                    }
                } else if (jsonNode.has("name")) {
                    result += SQLUtils.extractFunction(jsonNode).toString();


                } else {
                    result += "NULL";
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (type == EXPRESSION) {
//            result += "\ntype: expression \n\t\tleft: " + left.toString() + " \n\\t\t" + right.toString();
            result += "( " + left.toString() + " " + this.expressionFunction + " " + right.toString() + " ) ";
        }
        return result;
    }
}
