import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.leads.processor.query.QueryState;
import eu.leads.processor.query.SQLQuery;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/31/13
 * Time: 8:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class ObjectNodeTest {
    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            SQLQuery query = new SQLQuery("user", "location", "select * from atable", "SELECT");
            String val = mapper.writeValueAsString(query);
            JsonNode root = mapper.readTree(val);
            ((ObjectNode) root).put("state", String.valueOf(QueryState.COMPLETED));
            System.out.println("node: " + root.toString());
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

}
