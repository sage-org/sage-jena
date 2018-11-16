package org.gdd.sage.core.analyzer;

import org.apache.jena.sparql.expr.*;

import java.util.Deque;
import java.util.LinkedList;

public class FilterFormatter implements ExprVisitor {
    private String value;
    private Deque<String> stack;

    public FilterFormatter() {
        value = "";
        stack = new LinkedList<>();
    }

    public String getValue() {
        return stack.getFirst();
    }


    @Override
    public void visit(ExprFunction0 func) {
    }

    @Override
    public void visit(ExprFunction1 func) {
        func.getArg().visit(this);
        stack.push('(' + func.getFunctionName(null) + '(' + stack.pop() + "))");
    }

    @Override
    public void visit(ExprFunction2 func) {
        func.getArg2().visit(this);
        func.getArg1().visit(this);
        String funName = func.getFunctionName(null);
        switch (funName) {
            case "lt":
                stack.push('(' + stack.pop() + " < " + stack.pop() + ')');
                break;
            case "le":
                stack.push('(' + stack.pop() + " <= " + stack.pop() + ')');
                break;
            case "gt":
                stack.push('(' + stack.pop() + " > " + stack.pop() + ')');
                break;
            case "ge":
                stack.push('(' + stack.pop() + " >= " + stack.pop() + ')');
                break;
            case "and":
                stack.push('(' + stack.pop() + " && " + stack.pop() + ')');
                break;
            case "or":
                stack.push('(' + stack.pop() + " || " + stack.pop() + ')');
                break;
            default:
                stack.push('('  + funName +'(' + stack.pop() + ',' + stack.pop() + "))");
                break;
        }

    }

    @Override
    public void visit(ExprFunction3 func) {

    }

    @Override
    public void visit(ExprFunctionN func) {

    }

    @Override
    public void visit(ExprFunctionOp funcOp) {

    }

    @Override
    public void visit(NodeValue nv) {
        stack.push(nv.toString());
    }

    @Override
    public void visit(ExprVar nv) {
        stack.push(nv.toString());
    }

    @Override
    public void visit(ExprAggregator eAgg) {

    }

    @Override
    public void visit(ExprNone exprNone) {

    }
}
