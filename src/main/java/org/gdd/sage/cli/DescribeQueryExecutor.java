package org.gdd.sage.cli;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.Template;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Execute a SPARQL DESCRIBE query and output results in stdin
 * @author Thomas Minier
 */
public class DescribeQueryExecutor extends ConstructQueryExecutor {
    public DescribeQueryExecutor(String format) {
        super(format);
    }

    @Override
    protected Model evaluate(Dataset dataset, Query query) {
        // rewrite the DESCRIBE query into a CONSTRUCT query
        List<Var> qVars = query.getProjectVars();
        ElementGroup qBody = (ElementGroup) query.getQueryPattern();
        Query newQuery = new Query();
        newQuery.setQueryConstructType();

        // build description triple patterns
        List<Triple> describeTriples = qVars.parallelStream().map(var -> {
            return new Triple(var, Var.alloc("p_describe_" + var.getVarName()), Var.alloc("o_describe_" + var.getVarName()));
        }).collect(Collectors.toList());

        // build construct template
        BasicPattern bgp = new BasicPattern();
        for(Triple t: describeTriples) {
            bgp.add(t);
        }
        Template tmp = new Template(bgp);
        newQuery.setConstructTemplate(tmp);

        // build WHERE clause
        for(Triple t: describeTriples) {
            qBody.addTriplePattern(t);
        }
        newQuery.setQueryPattern(qBody);

        try (QueryExecution qexec = QueryExecutionFactory.create(newQuery, dataset)) {
            return qexec.execConstruct();
        }
    }
}
