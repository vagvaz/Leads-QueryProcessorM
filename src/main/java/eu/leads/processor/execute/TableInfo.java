package eu.leads.processor.execute;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 11/3/13
 * Time: 11:40 PM
 * To change this template use File | Settings | File Templates.
 */
@JsonAutoDetect

public class TableInfo {
    private Table table;
    private List<String> tableOptionsStrings;
    private final Map<String, ColumnDefinition> columnDefinitions;

    @JsonCreator
    public TableInfo(@JsonProperty("table") Table table, @JsonProperty("tableOptionsStrings") List<String> tableOptionsStrings, @JsonProperty("columnDefinitions") List<ColumnDefinition> columns) {

        columnDefinitions = new HashMap<String, ColumnDefinition>();
        setTable(table);
        setTableOptionsStrings(tableOptionsStrings);
        setColumnDefinitions(columns);
    }

    public TableInfo(Table table) {
        setTable(table);
        tableOptionsStrings = new ArrayList<String>();
        columnDefinitions = new HashMap<String, ColumnDefinition>();
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public List<String> getTableOptionsStrings() {
        return tableOptionsStrings;
    }

    public void setTableOptionsStrings(List<String> tableOptionsStrings) {
        this.tableOptionsStrings = tableOptionsStrings;
    }

    public Collection<ColumnDefinition> getColumnDefinitions() {
        return columnDefinitions.values();
    }

    public void setColumnDefinitions(List<ColumnDefinition> columnDefinitions) {
        for (ColumnDefinition col : columnDefinitions)
            this.columnDefinitions.put(col.getColumnName(), col);
    }

    @JsonIgnore
    public void addColumn(ColumnDefinition col) {
        columnDefinitions.put(col.getColumnName(), col);
    }

    @JsonIgnore
    public void removeColumn(String columnName) {
        columnDefinitions.remove(columnName);

    }

    @JsonIgnore
    public void removeOption(String optionName) {
        tableOptionsStrings.remove(optionName);
    }

    @JsonIgnore
    public void addOption(String option) {
        tableOptionsStrings.add(option);
    }

    @JsonIgnore
    public boolean hasColumn(String columnName) {
        return columnDefinitions.containsKey(columnName);
    }

    public String getColumnType(String columnName) {
        return columnDefinitions.get(columnName).getColDataType().getDataType();
    }

    @Override
    public String toString() {
        String result = table.toString() + " ";
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, ColumnDefinition> entry : columnDefinitions.entrySet()) {
            builder.append(entry.getKey() + " --> " + entry.getValue() + "\n");
        }
        return result + builder.toString();
    }
}
