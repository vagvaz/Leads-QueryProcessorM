package eu.leads.processor.execute.operators;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/22/13
 * Time: 6:46 PM
 * To change this template use File | Settings | File Templates.
 */

public enum OperatorType {
    READ,
    PROJECT,
    RENAME,
    JOIN,
    GROUPBY,
    SORT,
    DISTINCT,
    FILTER,
    LIMIT,
    OUTPUT,
    NONE;


    public static OperatorType fromString(String s) {

        OperatorType result = NONE;

        if (s.equals("READ"))
            result = READ;
        else if (s.equals("PROJECT"))
            result = PROJECT;
        else if (s.equals("RENAME"))
            result = RENAME;
        else if (s.equals("JOIN"))
            result = JOIN;
        else if (s.equals("GROUPBY"))
            result = GROUPBY;
        else if (s.equals("SORT"))
            result = SORT;
        else if (s.equals("DISTINCT"))
            result = DISTINCT;
        else if (s.equals("FILTER"))
            result = FILTER;
        else if (s.equals("LIMIT"))
            result = LIMIT;
        else if (s.equals("OUTPUT"))
            result = OUTPUT;

        return result;
    }

    public static String toString(OperatorType op) {
        String result = "";
        switch (op) {
            case READ:
                result = "READ";
                break;
            case PROJECT:
                result = "PROJECT";
                break;
            case RENAME:
                result = "RENAME";
                break;
            case JOIN:
                result = "JOIN";
                break;
            case GROUPBY:
                result = "GROUPBY";
                break;
            case SORT:
                result = "SORT";
                break;
            case DISTINCT:
                result = "DISTINCT";
                break;
            case FILTER:
                result = "FILTER";
                break;
            case LIMIT:
                result = "LIMIT";
                break;
            case OUTPUT:
                result = "OUTPUT";
                break;
            default:
                result = "UNKNOWN";
                break;
        }
        return result;
    }
}

