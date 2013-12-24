package eu.leads.processor.utils;

import eu.leads.processor.execute.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/4/13
 * Time: 2:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class Utilities {
    public static final Random r = new Random(System.currentTimeMillis());
    public static final String[] strings = {"a", "this", "wraia","ur","pdd","ab","thisb", "wraiax","urx","px","ax","thisa", "wraiaa","ura","pa","aa","thisw", "wraiaw","urw","ps","a2","thisd", "wraia1","ur2","p2"};

    public static List<Tuple> generateTuples(String[] names, String[] types, int number) {
        List<Tuple> result = new ArrayList<Tuple>();
        for (int i = 0; i < number; i++) {
            result.add(generateTuple(names, types));
        }
        return result;
    }

    public static Tuple generateTuple(String[] names, String[] types) {
        Tuple tuple = new Tuple("{}");
        assert (names.length == types.length);
        for (int i = 0; i < names.length; i++) {
            tuple.setAttribute(names[i], generateValue(types[i]));
        }
        return tuple;
    }

    public static String generateValue(String type) {
        if (type.equals("double")) {
            Double d = r.nextDouble();
            return d.toString();
        }
        if (type.equals("string")) {
            int index = Math.abs(r.nextInt()) % strings.length;
            return strings[index];
        }
        if (type.equals("int")) {
            Integer i = Math.abs(r.nextInt(10));
            return i.toString();
        }
        return "empty";

    }

    public static void printMap(Map<?, ?> map) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            System.out.println("key: " + entry.getKey() + "\tvalue: " + entry.getValue());
        }
    }
}
