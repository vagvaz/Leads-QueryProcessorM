import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.TableInfo;
import eu.leads.processor.plan.ExecutionPlan;
import eu.leads.processor.plan.SelectExtractor;
import eu.leads.processor.query.QueryContext;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 3:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class QueryContextTest {
    public static void main(String[] args) {
        String create = "create table test(id int not null primary key, url varchar(50), body varchar(100), pagerank double )";
        String select = "select url,pagerank from test where id > 1 group by url order by pagerank limit 3";

        ObjectMapper mapper = new ObjectMapper();
        CCJSqlParserManager manager = new CCJSqlParserManager();
        QueryContext context = new QueryContext();
        try {
            Statement c = manager.parse(new StringReader(create));
            Statement s = manager.parse(new StringReader(select));
            String cjson = mapper.writeValueAsString(c);
            JsonNode root = mapper.readTree(cjson);
            CreateTable ct = mapper.readValue(cjson.toString(), CreateTable.class);
//            Table t = ct.getTable();
            TableInfo ti = new TableInfo(ct.getTable(), null, ct.getColumnDefinitions());
            context.addTable(ti);

            String sjson = mapper.writeValueAsString(s);
            root = mapper.readTree(sjson);
            SelectExtractor extractor = new SelectExtractor(root);
            ExecutionPlan p = extractor.extractPlan("theoutput", context);
            System.out.println("plan " + p.toString());
            System.out.println("c " + cjson);
            String qjson = mapper.writeValueAsString(context);
            QueryContext qc = mapper.readValue(qjson, QueryContext.class);
            System.out.println("Before " + context.toString() + "\nAfter  " + qc.toString() + "\nequal " + (context.toString().equals(qc.toString())));
        } catch (JSQLParserException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
