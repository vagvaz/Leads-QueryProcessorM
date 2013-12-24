package eu.leads.processor.execute.operators;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 9/22/13
 * Time: 6:56 PM
 * To change this template use File | Settings | File Templates.
 */
public enum OperatorSubType {
    MAPREDUCE,
    STREAM;

    public static String toString(OperatorSubType subtype) {
        String result = "";
        switch (subtype) {
            case MAPREDUCE:
                result = "MAPREDUCE";
                break;
            case STREAM:
                result = "STREAM";
                break;
        }
        return result;
    }

    public static OperatorSubType fromString(Object s) {
        OperatorSubType result = MAPREDUCE;
        if (s.equals("MAPREDUCE"))
            result = MAPREDUCE;
        else if (s.equals("STREAM"))
            result = STREAM;
        return result;
    }
}

