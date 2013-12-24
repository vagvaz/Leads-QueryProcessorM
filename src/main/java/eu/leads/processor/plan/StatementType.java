package eu.leads.processor.plan;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/21/13
 * Time: 1:27 PM
 * To change this template use File | Settings | File Templates.
 */
public enum StatementType {
    SELECT,
    INSERT,
    DELETE,
    CREATETABLE,
    UPDATE;

    public static StatementType getType(String type) throws Exception {
        if (type.equals("SELECT"))
            return SELECT;
        else if (type.equals("INSERT"))
            return INSERT;
        else if (type.equals("CREATE TABLE"))
            return CREATETABLE;
        else
            throw new Exception("Invalid sql type ");

    }
}
