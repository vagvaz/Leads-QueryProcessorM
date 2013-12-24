import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import eu.leads.processor.utils.SQLUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/28/13
 * Time: 11:48 AM
 * To change this template use File | Settings | File Templates.
 */
public class SelectItemsTest {
    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        String json = "";
//        StringReader reader = new StringReader(json);

        json = "SELECT ble.f,ble.t,COUNT(ble.f,ble.g) as klkl FROM testble join jointable on f=s join j2 on j2.f=ble.g WHERE foo > 10 and (foo < 15 and fle like \'adidas\') group by ble.f,ble.g having COUNT(ble.f,ble.g) > 5 and COUNT(ble.f,ble.g) = 7 ORDER BY ble.g ASC,ble.f DESC,ble.p DESC limit 5";
        CCJSqlParserManager manager = new CCJSqlParserManager();
        ObjectWriter writer = mapper.writer().withDefaultPrettyPrinter();
        try {
            Statement st = manager.parse(new StringReader(json));

            String value = writer.writeValueAsString(st);
            System.out.println("PRETTY\n" + value + "\nendpretty");
            // System.out.println("yupi!!!\n"+value);
            JsonNode root = mapper.readTree(value);

            JsonNode selectColumns = root.path("selectBody").path("selectItems");
            Iterator<JsonNode> items = selectColumns.iterator();
            List<Column> columns = new ArrayList<Column>();
            List<Table> allTable = new ArrayList<Table>();
            List<Function> functions = new ArrayList<Function>();
            while (items.hasNext()) {

                JsonNode expr = items.next();
                System.out.println("expr:: " + expr.toString());
//                if(columns.size() == 0)
//                continue;
                if (expr.has("expression")) {
                    expr = expr.path("expression");
                    if (!expr.has("name")) {
                        Column selected = mapper.readValue(expr.toString(), Column.class);
                        columns.add(selected);
                    } else {

//                        expr.findParent("expression");
                        Function selected = SQLUtils.extractFunction(expr);
                        functions.add(selected);
                    }
                } else {
                    if (!expr.has("table")) {
                        columns.clear();
                        break;
                    } else {
                        expr = expr.path("table");
                        Table table = mapper.readValue(expr.toString(), Table.class);
                        allTable.add(table);
                    }
                }
            }
            if (columns.size() == 0)
                System.out.println("All Columns");
            for (Column c : columns) {
                System.out.println("selected " + c.toString());
            }
            for (Table c : allTable) {
                System.out.println("selected tables " + c.toString());
            }
            for (Function f : functions) {
                System.out.println("selected f " + f.toString());
            }

            JsonNode orderby = root.path("selectBody").path("orderByElements");
            Iterator<JsonNode> iterator = orderby.iterator();
            List<Boolean> ascendingOrder = new ArrayList<Boolean>();
            List<Column> orderByColumns = new ArrayList<Column>();
            while (iterator.hasNext()) {
                JsonNode expr = iterator.next();

                ascendingOrder.add(expr.path("asc").asBoolean());
                if (expr.has("expression")) {
                    expr = expr.path("expression");
                    if (expr.has("columnName")) {

                        Column selected = mapper.readValue(expr.toString(), Column.class);
                        orderByColumns.add(selected);
                    }

                }
            }
            for (int i = 0; i < orderByColumns.size(); i++) {
                System.out.println("columnt: " + orderByColumns.get(i) + " " + ascendingOrder.get(i));
            }
//            t = mapper.readValue(String.valueOf(from),Table.class);
//            System.out.println("t: " + t.getName() + " " + (t.getAlias() == null) );
        } catch (JSQLParserException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (JsonProcessingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
