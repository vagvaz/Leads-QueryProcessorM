package eu.leads.processor.execute.operators;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.LeadsMapper;
import eu.leads.processor.execute.Tuple;
import net.sf.jsqlparser.schema.Column;
import org.infinispan.distexec.mapreduce.Collector;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 12/3/13
 * Time: 10:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class SortMapper extends LeadsMapper<String, String, String, String> {

    transient public List<Column> sortColumns;
    Integer counter = 0;
    Integer numParts = 0;

    public SortMapper(Properties configuration) {
        super(configuration);
    }

    public void initialize() {
        counter = 0;
        isInitialized = true;
        super.initialize();
        String columns = conf.getProperty("sortColumns");
        numParts = Integer.parseInt(conf.getProperty("parts"));
        ObjectMapper mapper = new ObjectMapper();
        try {
            sortColumns = mapper.readValue(columns, new TypeReference<List<Column>>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(String key, String value, Collector<String, String> collector) {
        if (!isInitialized)
            initialize();
        progress();
//        Tuple tuple = new Tuple(value);
        Integer outkey = counter % numParts;
        collector.emit(outkey.toString(), value);
        counter = counter + 1;
    }
}
