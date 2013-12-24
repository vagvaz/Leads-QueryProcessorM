package eu.leads.processor.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.leads.processor.execute.TableInfo;
import eu.leads.processor.utils.InfinispanUtils;
import net.sf.jsqlparser.schema.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/5/13
 * Time: 10:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class QueryContext {
    private Map<String, TableInfo> tables;
    private String queryId;

    public QueryContext() {
        tables = new HashMap<String, TableInfo>();
    }

    public QueryContext(Query query) {
        this.queryId = query.getId();
        tables = new HashMap<String, TableInfo>();
    }

    public String getQueryId() {
        return queryId;

    }

    public void setQueryId(String id) {
        this.queryId = id;
    }

    public Map<String, TableInfo> getTables() {
        return tables;
    }

    public void setTables(Map<String, TableInfo> tables) {
        this.tables = tables;
    }

    public void addTable(Table table) {
        Map<String, String> tableCache = InfinispanUtils.getOrCreatePersistentMap("tables");
        ObjectMapper mapper = new ObjectMapper();
        TableInfo ti = null;
        try {
            String tiValue = tableCache.get("tables:" + table.getName());
            if (tiValue != null)
                ti = mapper.readValue(tiValue, TableInfo.class);
            else
                ti = new TableInfo(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (table.getAlias() == null) {
            if (!tables.containsKey(table.getName()))
                tables.put(table.getName(), ti);
        } else {
            if (!tables.containsKey(table.getAlias()))
                tables.put(table.getAlias(), ti);
        }
    }

    public void addTable(TableInfo info) {
        if (info.getTable().getAlias() == null) {

            tables.put(info.getTable().getName(), info);
        } else {
            tables.put(info.getTable().getAlias(), info);
        }
    }

    public String resolveTableName(String columnName) {
        for (Map.Entry<String, TableInfo> entry : tables.entrySet()) {
            if (entry.getValue().hasColumn(columnName))
                return entry.getKey();
        }
        return "";
    }

    public String getColumnType(String columnName, String tableName) {
        String table = tableName;
        if (tableName == null)
            table = resolveTableName(columnName);
        return tables.get(table).getColumnType(columnName);
    }

    @Override
    public String toString() {
        String result = queryId;
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, TableInfo> entry : tables.entrySet()) {
            builder.append(entry.getKey() + ": " + entry.getValue() + "\n");
        }
        result += builder.toString();
        return result;
    }
}
