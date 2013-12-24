package eu.leads.processor.sql;

import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.log4j.Logger;

public class QueryVisitor implements StatementVisitor {

    static final Logger log = Logger.getLogger(QueryVisitor.class.getName());
    private String statementType;

    public QueryVisitor() {
    }


    @Override
    public void visit(Select select) {
        this.statementType = "SELECT";
    }

    @Override
    public void visit(Delete delete) {
        this.statementType = "DELETE";
        delete.accept(this);
        log.error("delete is an unsupported operation");

    }

    @Override
    public void visit(Update update) {
        log.error("update is an unsupported operation");
        this.statementType = "UPDATE";
    }

    @Override
    public void visit(Insert insert) {
        log.error("insert is an unsupported operation");

    }

    @Override
    public void visit(Replace replace) {
        log.error("replace is an unsupported operation");

    }

    @Override
    public void visit(Drop drop) {
        log.error("drop is an unsupported operation");
    }

    @Override
    public void visit(Truncate truncate) {
        log.error("truncate is an unsupported operation");

    }

    @Override
    public void visit(CreateIndex index) {
        log.error("create index is an unsupported operation");

    }

    @Override
    public void visit(CreateTable table) {
        log.error("create table is an unsupported operation");

    }

    @Override
    public void visit(CreateView view) {
        log.error("create view is an unsupported operation");
    }

    public String getStatementType() {
        return statementType;
    }
}
