package eu.leads.processor.utils.math;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/7/13
 * Time: 4:42 PM
 * To change this template use File | Settings | File Templates.
 */
//Mathematical Utilities
public class MathUtils {
    public static Object add(Object o1, Object o2, String type) {
        if (type.equalsIgnoreCase("double") || type.equals("float")) {
            return add((Double) o1, (Double) o2);
        } else if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("long")) {
            return add((Long) o1, (Long) o2);
        } else if (type.equalsIgnoreCase("string")) {
            return ((String) o1).concat((String) o2);
        } else if (type.equalsIgnoreCase("date")) {
            return null;
        }
        return null;
    }

    private static Long add(Long o1, Long o2) {
        return o1 + o2;
    }

    private static Double add(Double o1, Double o2) {
        return o1 + o2;
    }


    public static Object divide(Object o1, Object o2, String type) {
        if (type.equalsIgnoreCase("double") || type.equalsIgnoreCase("float")) {
            return add((Double) o1, (Double) o2);
        } else if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("long")) {
            return add((Long) o1, (Long) o2);
        } else if (type.equalsIgnoreCase("string")) {
            return "";
        } else if (type.equalsIgnoreCase("date")) {
            return null;
        }
        return null;
    }

    private static Long divide(Long o1, Long o2) {
        return o1 / o2;
    }

    private static Double divide(Double o1, Double o2) {
        return o1 / o2;
    }


    public static Object compare(Object o1, Object o2, String type) {
        if (type.equalsIgnoreCase("double") || type.equalsIgnoreCase("float")) {
            return compare((Double) o1, (Double) o2);
        } else if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("long")) {
            return compare((Long) o1, (Long) o2);
        } else if (type.equalsIgnoreCase("string")) {
            return ((String) o1).compareTo((String) o2);
        } else if (type.equalsIgnoreCase("date")) {
            return compare((Date) o1, (Date) o2);
        }
        return 0;
    }

    public static int compare(Long o1, Long o2) {
        return o1.compareTo(o2);
    }

    public static int compare(Double o1, Double o2) {
        return o1.compareTo(o2);
    }

    public static int compare(Date d1, Date d2) {
        return d1.compareTo(d2);
    }

    public static String handleType(String t1) {
        if (t1.equalsIgnoreCase("double") || t1.equalsIgnoreCase("float")) {
            return "double";
        } else if (t1.equalsIgnoreCase("int") || t1.equalsIgnoreCase("long"))
            return "long";
        else
            return "string";
    }

    public static String handleTypes(String t1, String t2) {
        if (t1.equalsIgnoreCase("double") || t1.equalsIgnoreCase("float")) {
            if (t2.equalsIgnoreCase("double") || t2.equalsIgnoreCase("float")) {
                return "double";
            } else if (t2.equalsIgnoreCase("int") || t2.equalsIgnoreCase("long")) {
                return "double";
            } else {
                return "string";
            }
        }
        return "string";
    }

    public static boolean isArithmentic(String type) {
        return type.equalsIgnoreCase("double") || type.equals("int") || type.equalsIgnoreCase("float") || type.equalsIgnoreCase("long");
    }
}
