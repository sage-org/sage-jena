package org.gdd.sage.http.results;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.http.data.SageStatistics;

import java.util.LinkedList;
import java.util.List;

/**
 * Results of evaluating a SPARQL INSERT DATA or DELETE DATA against a Sage server
 * @author Thomas Minier
 */
public class UpdateResults implements SageQueryResults {
    private List<Quad> processedQuads;
    private SageStatistics stats;
    private String error;
    private boolean hasError;
    private static final Var SUBJECT_VARIABLE = Var.alloc("s");
    private static final Var PREDICATE_VARIABLE = Var.alloc("p");
    private static final Var OBJECT_VARIABLE = Var.alloc("o");
    private static final Var GRAPH_VARIABLE = Var.alloc("graph");

    /**
     * Constructor
     * @param defaultGraphURI - URI of the default RDF graph
     * @param bindings - Bindings received from the server after the query evaluation
     * @param stats - Statistics related to the query evaluation
     */
    public UpdateResults(String defaultGraphURI, List<Binding> bindings, SageStatistics stats) {
        processedQuads = new LinkedList<>();
        for(Binding binding: bindings) {
            if (binding.contains(SUBJECT_VARIABLE) && binding.contains(PREDICATE_VARIABLE)
                    && binding.contains(OBJECT_VARIABLE) && binding.contains(GRAPH_VARIABLE)) {
                // replace the URI of the default RDF graph by the constant used by Apache Jena
                Node graph = binding.get(GRAPH_VARIABLE);
                if (graph.getURI().equals(defaultGraphURI)) {
                    graph = NodeFactory.createURI("urn:x-arq:DefaultGraphNode");
                }
                Quad quad = new Quad(graph, binding.get(SUBJECT_VARIABLE), binding.get(PREDICATE_VARIABLE), binding.get(OBJECT_VARIABLE));
                processedQuads.add(quad);
            }
        }
        this.stats = stats;
        this.error = "No error during query evaluation";
        this.hasError = false;
    }

    private UpdateResults(String error) {
        this.error = error;
        this.hasError = true;
    }

    public static UpdateResults withError(String error) {
        return new UpdateResults(error);
    }

    public List<Quad> getProcessedQuads() {
        return processedQuads;
    }

    @Override
    public SageStatistics getStats() {
        return stats;
    }

    @Override
    public String getError() {
        return error;
    }

    @Override
    public boolean hasError() {
        return hasError;
    }
}
