import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/4/13
 * Time: 5:59 PM
 * To change this template use File | Settings | File Templates.
 */

public class JacksonTest {

    public static void main(String[] args) throws IOException {
//        Column c = new Column(new Table("s","g"),"b");
//        Column c1 = new Column(new Table("s","g"),"d");
//        Column c2 = new Column(new Table("s","g"),"f");
//        Column c3 = new Column(new Table("s","g"),"h");
//        Map<String,Column> map = new HashMap<String,Column>();
//        List<Column> list = new LinkedList<Column>();
//        list.add(c);
//        list.add(c1);
//        list.add(c2);
//        list.add(c3);
//
//        for (Iterator<Column> iterator = list.iterator(); iterator.hasNext(); ) {
//            Column next = iterator.next();
//
//            map.put(next.getWholeColumnName(),next);
//        }
//        ComplexType ct = new ComplexType();
//        ct.setColumns(list);
//        ct.setTest(map);
        ObjectMapper mapper = new ObjectMapper();

        //mapper.configure(SerializationFeature.WRAP_ROOT_VALUE,true);
        //mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE,true);
//        String json = mapper.writeValueAsString(ct);
        String json = "";

        System.out.println(json);
//        StringReader reader = new StringReader(json);
//        ComplexType at = mapper.readValue(reader,ComplexType.class);
//        System.out.println(at.toString());
        json = "SELECT *,ble.f,ble.t,ble.* FROM testble join jointable on f=s join j2 on j2.f=ble.g WHERE foo > 10 and (foo < 15 and fle like \'adidas\') group by ble.f ORDER BY ble.g limit 5";
        CCJSqlParserManager manager = new CCJSqlParserManager();
        ObjectWriter writer = mapper.writer().withDefaultPrettyPrinter();
        try {
            Statement st = manager.parse(new StringReader(json));

            String value = writer.writeValueAsString(st);
            System.out.println("PRETTY\n" + value + "\nendpretty");
            // System.out.println("yupi!!!\n"+value);
//            HashMap<String, Statement> foo = new HashMap<String, Statement>();
            JsonNode root = mapper.readTree(value);

            JsonNode select = root.path("selectBody").path("selectItems");
            Iterator<JsonNode> s = select.elements();
            while (s.hasNext()) {
                JsonNode n = s.next();
                Iterator<String> ss = n.fieldNames();
                while (ss.hasNext())
                    System.out.println("ss: " + ss.next());
                System.out.println("node: " + n.toString());
            }
            System.out.println(select.toString());

            JsonNode from = root.path("selectBody").path("joins");
            JsonNode joins = root.path("selectBody").path("joins");
            if (!joins.isNull()) {
                System.out.println("has jjoints " + joins.toString());
                Iterator<JsonNode> iterator = joins.elements();
                while (iterator.hasNext()) {
                    JsonNode joinRoot = iterator.next();
                    System.out.println("------> " + joinRoot.toString());
                }
            }
//            Table t = new Table();
            System.out.println("text value " + (from.textValue() == null));
//            t = mapper.readValue(String.valueOf(from),Table.class);
//            System.out.println("t: " + t.getName() + " " + (t.getAlias() == null) );
        } catch (JSQLParserException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
//        try {
//            json = "CREATE TABLE lefteria(sel int primary key,vim varchar(100) )";
//            Statement st = manager.parse(new StringReader(json));
//
//            String value = writer.writeValueAsString(st);
//            System.out.println("yupi1!!!\n"+value);
//        } catch (JSQLParserException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//        try {
//            json = "INSERT INTO tab values(1,2,3,'llll')";
//            Statement st = manager.parse(new StringReader(json));
//            String value = writer.writeValueAsString(st);
//            System.out.println("yup2i!!!\n"+value);
//        } catch (JSQLParserException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
    }
}
