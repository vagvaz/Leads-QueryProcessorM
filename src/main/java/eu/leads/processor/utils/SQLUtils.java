package eu.leads.processor.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/28/13
 * Time: 2:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class SQLUtils {
    public static ExpressionList extractExpressionList(JsonNode root) {
        ExpressionList result = new ExpressionList();
        List<Expression> columns = new ArrayList<Expression>();
        Iterator<JsonNode> iterator = root.iterator();
        ObjectMapper mapper = new ObjectMapper();
        while (iterator.hasNext()) {
            JsonNode expr = iterator.next();
            if (expr.has("columnName")) {
                Column selected = null;
                try {
                    selected = mapper.readValue(expr.toString(), Column.class);
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                columns.add((Expression) selected);
            } else {
                if (!expr.has("table")) {
                    columns.clear();
                    break;
                }

            }
        }
        result.setExpressions(columns);
        return result;
    }

    public static Function extractFunction(JsonNode root) {
        Function result = new Function();
        result.setName(root.path("name").asText());
        result.setAllColumns(root.path("allColumns").asBoolean());
        result.setEscaped(root.path("escaped").asBoolean());
        if (!root.path("parameters").isNull()) {
            ExpressionList ee = SQLUtils.extractExpressionList(root.path("parameters").path("expressions"));
            result.setParameters(ee);
        }
        return result;
    }

    public static List<Column> extractColumns(JsonNode root) {
        List<Column> columns = new ArrayList<Column>();
        Iterator<JsonNode> iterator = root.iterator();
        ObjectMapper mapper = new ObjectMapper();
        while (iterator.hasNext()) {
            JsonNode expr = iterator.next();
            if (expr.has("columnName")) {
                Column selected = null;
                try {
                    selected = mapper.readValue(expr.toString(), Column.class);
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                columns.add(selected);
            }
        }
        return columns;
    }

    public static String getSQLType(String query) {
        String tmp = query.trim().toUpperCase();
        if (tmp.startsWith("SELECT"))
            return "SELECT";
        else if (tmp.startsWith("INSERT"))
            return "INSERT";
        else if (tmp.startsWith("CREATE TABLE"))
            return "CREATE TABLE";
        else
            return "INVALID";
    }
}
