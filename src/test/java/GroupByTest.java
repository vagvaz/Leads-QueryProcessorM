import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import net.sf.jsqlparser.JSQLParserException;
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
 * Time: 12:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class GroupByTest {
    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        String json = "";
//        StringReader reader = new StringReader(json);

        json = "SELECT ble.f,ble.t,ble.* FROM testble join jointable on f=s join j2 on j2.f=ble.g WHERE foo > 10 and (foo < 15 and fle like \'adidas\') group by ble.f,ble.g,ble.p ORDER BY ble.g limit 5";
        CCJSqlParserManager manager = new CCJSqlParserManager();
        ObjectWriter writer = mapper.writer().withDefaultPrettyPrinter();
        try {
            Statement st = manager.parse(new StringReader(json));

            String value = writer.writeValueAsString(st);
            System.out.println("PRETTY\n" + value + "\nendpretty");
            // System.out.println("yupi!!!\n"+value);
            JsonNode root = mapper.readTree(value);

            JsonNode selectColumns = root.path("selectBody").path("groupByColumnReferences");
            Iterator<JsonNode> items = selectColumns.iterator();
            List<Column> columns = new ArrayList<Column>();
            List<Table> allTable = new ArrayList<Table>();

            while (items.hasNext()) {
                JsonNode expr = items.next();
                Column selected = mapper.readValue(expr.toString(), Column.class);
                columns.add(selected);

            }

            if (columns.size() == 0)
                System.out.println("All Columns");
            for (Column c : columns) {
                System.out.println("selected " + c.toString());
            }
            for (Table c : allTable) {
                System.out.println("selected tables " + c.toString());
            }
//            t = mapper.readValue(String.valueOf(from),Table.class);
//            System.out.println("t: " + t.getName() + " " + (t.getAlias() == null) );

        } catch (JsonProcessingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (JSQLParserException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
