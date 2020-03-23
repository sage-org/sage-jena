package org.gdd.sage.engine.update.queries;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.engine.update.base.UpdateQuery;

import java.util.LinkedList;
import java.util.List;

/**
 * A Base class to implements an evaluator for SPARQL DELETE/INSERT queries
 * @author Thomas Minier
 */
public abstract class BaseDeleteInsert implements UpdateQuery {
    /**
     * Instantiate a list of templates (RDF quads) using a set of solution bindings.
     * Exclude quads that where not all variables were substituted.
     * @param templates - Templates, i.e., RDF quads with SPARQL variables
     * @param bindings - List of sets of solution bindings
     * @return The list of instantiated RDF quads
     */
    protected List<Quad> buildQuadsTemplates(List<Quad> templates, List<Binding> bindings) {
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
     * Instantiate a list of templates (triple patterns) using a set of solution bindings.
     * Exclude triple patterns that where not all variables were substituted.
     * @param templates - Templates, i.e., triple patterns with SPARQL variables
     * @param bindings - List of sets of solution bindings
     * @return The list of instantiated triple patterns
     */
    protected List<Triple> buildTripleTemplates(List<Triple> templates, List<Binding> bindings) {
        List<Triple> results = new LinkedList<>();
        for(Binding binding: bindings) {
            for(Triple template: templates) {
                Triple newTriple = Substitute.substitute(template, binding);
                // assert that all variables in the new quad were substituted
                if ((!newTriple.getSubject().isVariable()) && (!newTriple.getPredicate().isVariable()) && (!newTriple.getObject().isVariable())) {
                    results.add(newTriple);
                }
            }
        }
        return results;
    }
}
