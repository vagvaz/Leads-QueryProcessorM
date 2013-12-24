package eu.leads.processor.utils.math;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import eu.leads.processor.execute.Tuple;
import eu.leads.processor.query.QueryContext;

/**
 * Created with IntelliJ IDEA.
 * User: vagvaz
 * Date: 10/29/13
 * Time: 5:17 AM
 * To change this template use File | Settings | File Templates.
 */
//This class has the root node of the a tree produced by a where clause in a sql command
// for example let the where part of an sql query be where webpages.pagerank > 0.2
//the we create a tree with three nodes the root keeps the expression for '>' node and has two children
// the left one will read the value of the tuple for the pagerank and the right keeps the constant 0.2.
//All the job is done into the MathOperatorTreeNode
@JsonAutoDetect
@JsonDeserialize(converter = MathTreeOperatorConverter.class)
public class MathOperatorTree {
    public MathOperatorTree(MathOperatorTreeNode root) {
        this.root = root;
    }

    public MathOperatorTreeNode getRoot() {
        return root;
    }

    public void setRoot(MathOperatorTreeNode root) {
        this.root = root;
    }

    private MathOperatorTreeNode root;

    @JsonCreator
    public MathOperatorTree(@JsonProperty("root") JsonNode node) {
        root = new MathOperatorTreeNode(node);
    }

    @Override
    public String toString() {
        String result = root.toString();
        return result;
    }

    @JsonIgnore
    public boolean accept(Tuple tuple, QueryContext context) {
        return root.accept(tuple, context);
    }
}
