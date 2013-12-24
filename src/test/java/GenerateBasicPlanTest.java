import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import eu.leads.processor.plan.SelectExtractor;
import eu.leads.processor.query.Query;
import eu.leads.processor.query.QueryContext;
import eu.leads.processor.query.SQLQuery;
import eu.leads.processor.sql.Plan;
import eu.leads.processor.utils.SQLUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 7:42 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateBasicPlanTest {
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
            Query query = new SQLQuery("vagvaz", "nowhere", value, SQLUtils.getSQLType(json));


            SelectExtractor extractor = new SelectExtractor(root);
            QueryContext context = new QueryContext();
            context.setQueryId(query.getId());
            Plan p = extractor.extractPlan("myoutput", context);
            System.out.println("The Plan is " + p.toString());
//            t = mapper.readValue(String.valueOf(from),Table.class);
//            System.out.println("t: " + t.getName() + " " + (t.getAlias() == null) );
        } catch (JSQLParserException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (JsonProcessingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
