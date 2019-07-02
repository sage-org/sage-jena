package org.gdd.sage.engine.update.queries;


import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.modify.request.UpdateModify;
import org.gdd.sage.engine.update.base.UpdateQuery;

import java.util.LinkedList;
import java.util.List;

/**
 * Execute a DELETE/INSERT query
 * @see {@href https://www.w3.org/TR/2013/REC-sparql11-update-20130321/#deleteInsert}
 * @author Thomas Minier
 */
public class DeleteInsertQuery implements UpdateQuery {
    private ResultSet source;
    private QueryExecution execution;
    private UpdateQuery deleteQuery;
    private UpdateQuery insertQuery;
    private List<Quad> deleteTemplates;
    private List<Quad> insertTemplates;
    private List<Binding> results;
    private boolean warmup;
    private int bucketSize;

    /**
     * Constructor
     * @param execution - Source of solution bindings from the evaluation of the WHERE clause
     * @param deleteTemplates - Delete templates, i.e., RDF quads with SPARQL variables
     * @param insertTemplates - Insert templates, i.e., RDF quads with SPARQL variables
     * @param bucketSize - Bucket size, i.e., how many RDF triples are sent by query
     */
    private DeleteInsertQuery(QueryExecution execution, List<Quad> deleteTemplates, List<Quad> insertTemplates, int bucketSize) {
        this.source = execution.execSelect();
        this.execution = execution;
        this.deleteQuery = new EmptyQuery();
        this.insertQuery = new EmptyQuery();
        this.results = new LinkedList<>();
        this.deleteTemplates = new LinkedList<>(deleteTemplates);
        this.insertTemplates = new LinkedList<>(insertTemplates);
        this.bucketSize = bucketSize;
        this.warmup = true;
    }

    /**
     * Build an {@link DeleteInsertQuery} from a logical query node
     * @param update - Logical query node
     * @param dataset - RDF dataset used to evaluate the query
     * @param bucketSize - Bucket size, i.e., how many RDF triples are sent by query
     * @return An new {@link DeleteInsertQuery}
     */
    public static DeleteInsertQuery fromOperation(UpdateModify update, Dataset dataset, int bucketSize) {
        // get templates
        List<Quad> deleteTemplates = new LinkedList<>();
        List<Quad> insertTemplates = new LinkedList<>();
        if (update.hasDeleteClause()) {
            deleteTemplates.addAll(update.getDeleteQuads());
        }
        if (update.hasInsertClause()) {
            insertTemplates.addAll(update.getInsertQuads());
        }
        // build a SPARQL query from the WHERE clause
        Op where = Algebra.compile(update.getWherePattern());
        Query query = QueryFactory.create(OpAsQuery.asQuery(where));
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
        return new DeleteInsertQuery(qexec, deleteTemplates, insertTemplates, bucketSize);
    }

    /**
     * Instantiate a list of templates (RDF quads) using a set of solution bindings.
     * Exclude quads that where not all variables were substituted.
     * @param templates - Templates, i.e., RDF quads with SPARQL variables
     * @param bindings - List of sets of solution bindings
     * @return The list of instantiated RDF quads
     */
    private List<Quad> buildTemplates(List<Quad> templates, List<Binding> bindings) {
        List<Quad> results = new LinkedList<>();
        for(Binding binding: bindings) {
            for(Quad template: templates) {
                Quad newQuad = Substitute.substitute(template, binding);
                // assert that all variables in the new quad were substituted
                if ((!newQuad.getSubject().isVariable()) && (!newQuad.getPredicate().isVariable()) && (!newQuad.getObject().isVariable())) {
                    results.add(newQuad);
                }
            }
        }
        return results;
    }

    /**
     * Build the next SPARQL UPDATE query to execute
     * @return A SPARQL UPDATE query, i.e., either an INSERT DATA or DELETE DATA query
     */
    @Override
    public String nextQuery() {
        UpdateQuery currentQuery;
        if (deleteQuery.hasNextQuery()) {
            currentQuery = deleteQuery;
        } else if (insertQuery.hasNextQuery()) {
            currentQuery = insertQuery;
        } else {
            return null;
        }
        return currentQuery.nextQuery();
    }

    /**
     * Test if the query has more RDF triples to process
     * @return True the query has more RDF triples to process, False otherwise
     */
    @Override
    public boolean hasNextQuery() {
        if (warmup) {
            // execute update pattern
            while(source.hasNext()) {
                results.add(source.nextBinding());
            }
            if (!results.isEmpty()) {
                // build delete templates
                List<Quad> deleteQuads = buildTemplates(deleteTemplates, results);
                if (!deleteQuads.isEmpty()) {
                    deleteQuery = new DeleteQuery(deleteQuads, bucketSize);
                }
                // build insert templates
                List<Quad> insertQuads = buildTemplates(insertTemplates, results);
                if (!insertQuads.isEmpty()) {
                    insertQuery = new InsertQuery(insertQuads, bucketSize);
                }
            }
            warmup = false;
        }
        return (!results.isEmpty()) && (deleteQuery.hasNextQuery() || insertQuery.hasNextQuery());
    }

    /**
     * Indicate that a list of quads has been processed by the server
     * @param quads - List of quads
     */
    public void markAsCompleted(List<Quad> quads) {
        if (deleteQuery.hasNextQuery()) {
            deleteQuery.markAsCompleted(quads);
        } else {
            insertQuery.markAsCompleted(quads);
        }
    }

    /**
     * Release all resources used by the object for processing
     */
    @Override
    public void close() {
        execution.close();
    }
}
