package org.gdd.sage.federated;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformBase;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.gdd.sage.federated.strategy.AskStrategy;
import org.gdd.sage.federated.strategy.SourceSelectionStrategy;
import org.gdd.sage.http.SageRemoteClient;

import java.util.*;

/**
 * Performs an source selection and a query decomposition to turn a SPARQL query into a federated SPARQL query.
 * This class also applies the "exclusive groups" and "join distribution over unions" optimizations.
 * @author Thomas Minier
 */
public class SourceSelection extends TransformBase {
    private Map<String, SageRemoteClient> httpClients;
    private SourceSelectionStrategy selectionStrategy;

    /**
     * Default constructor
     */
    public SourceSelection() {
        httpClients = new HashMap<>();
        selectionStrategy = new AskStrategy();
    }

    /**
     * Constructor with parametric source selection strategy
     * @param selectionStrategy - The source selection strategy to use
     */
    public SourceSelection(SourceSelectionStrategy selectionStrategy) {
        httpClients = new HashMap<>();
        this.selectionStrategy = selectionStrategy;
    }

    /**
     * Register a source, i.e., a RDF graph, in the federation
     * @param graphURI - Graph URI
     * @param httpClient - HTTP client used to access the graph
     */
    public void registerSource(String graphURI, SageRemoteClient httpClient) {
        if (!httpClients.containsKey(graphURI)) {
            httpClients.put(graphURI, httpClient);
        }
    }

    /**
     * Close all open connections with the remote graphs
     */
    public void close() {
        for(Map.Entry<String, SageRemoteClient> source: httpClients.entrySet()) {
            source.getValue().close();
        }
    }

    /**
     * Perform source selection and query decomposition of a SPARQL query, using the current registered sources.
     * @param query - SPARQL query to localize, in string format
     * @return Localized federated SPARQL query
     */
    public Op localize(String query) {
        return localize(QueryFactory.create(query));
    }

    /**
     * Perform source selection and query decomposition of a SPARQL query, using the current registered sources.
     * @param query - SPARQL query to localize, in Apache Jena internal representation
     * @return Localized federated SPARQL query
     */
    public Op localize(Query query) {
        Op plan = Algebra.compile(query);
        return Transformer.transform(this, plan);
    }

    @Override
    public Op transform(OpBGP opBGP) {
        List<Set<LocalizedPattern>> localizedPatterns = new LinkedList<>();

        // perform localization for each triple pattern and each source
        for(Triple pattern: opBGP.getPattern().getList()) {
            Set<LocalizedPattern> relevantSources = new HashSet<>();
            for(Map.Entry<String, SageRemoteClient> source: httpClients.entrySet()) {
                String graphURL = source.getKey();
                int cardinality = selectionStrategy.getCardinality(pattern, graphURL, source.getValue());
                if (cardinality > 0) {
                    relevantSources.add(new LocalizedPattern(graphURL, pattern, cardinality));
                }
            }
            // no relevant source => pattern has no matching RDF triples => bgp has no results
            if (relevantSources.isEmpty()) {
                return new OpBGP(new BasicPattern());
            }
            localizedPatterns.add(relevantSources);
        }

        List<Op> allJoins = new LinkedList<>();

        // apply join distribution (P1 UNION P2) JOIN P3 => (P1 JOIN P3) UNION (P2 JOIN P3)
        for(List<LocalizedPattern> joinGroup: Sets.cartesianProduct(localizedPatterns)) {
            // group patterns by source
            ImmutableListMultimap<String, LocalizedPattern> mapping = Multimaps.index(joinGroup, LocalizedPattern::getSource);

            List<ExclusiveGroup> exclusiveGroups = new LinkedList<>();
            List<LocalizedPattern> others = new LinkedList<>();

            // generate exclusive groups for this group pattern
            for(String source: mapping.keySet()) {
                ImmutableList<LocalizedPattern> group = mapping.get(source);

                // skip group of size 1, as they cannot create an exclusive group (by definition)
                if (group.size() == 1) {
                    others.add(group.get(0));
                } else {
                    ExclusiveGroup eg = new ExclusiveGroup(source);
                    // check that each pattern can be joined with at least one other pattern in the group
                    // TODO Do we need a double for loop to check for cartesian-free exclusive groups?
                    for(int i = 0; i < group.size(); i++) {
                        LocalizedPattern pattern = group.get(i);
                        Set<Var> patternVars = pattern.getVariables();
                        boolean isValid = false;
                        for(int j = 0; j < group.size(); j++) {
                            // avoid comparison of the pattern with itself
                            if (i != j) {
                                Set<Var> otherVars = group.get(j).getVariables();
                                // tp_1 can be joined with tp_2 iff vars(tp_1) intersection vars(tp_2) != empty set
                                isValid = isValid || (! Sets.intersection(patternVars, otherVars).isEmpty());
                            }
                        }
                        // if the triple can be joined without cartesian product, put it in the exclusive group
                        if(isValid) {
                            eg.addPattern(pattern);
                        } else {
                            // otherwise, exclude it from the exclusive group
                            others.add(pattern);
                        }
                    }
                    // register the final exclusive group (if not empty)
                    if (!eg.isEmpty()) {
                        exclusiveGroups.add(eg);
                    }
                }
            }

            // sort patterns without an exclusive group by cardinality
            Collections.sort(others);

            // build the final join expression, starting with exclusive group
            Op opJoin = null;
            if (!exclusiveGroups.isEmpty()) {
                opJoin = exclusiveGroups.get(0).toOp();
                exclusiveGroups.remove(0);
            }
            for(ExclusiveGroup eg: exclusiveGroups) {
                opJoin = OpJoin.create(opJoin, eg.toOp());
            }
            // then, add all remaining patterns
            if (opJoin == null) {
                opJoin = others.get(0).toOp();
                others.remove(0);
            }
            for(LocalizedPattern pattern: others) {
                opJoin = OpJoin.create(opJoin, pattern.toOp());
            }

            allJoins.add(opJoin);
        }

        // build the top-level union
        Op opUnion = allJoins.get(0);
        for(int ind = 1; ind < allJoins.size(); ind++) {
            opUnion = new OpUnion(opUnion, allJoins.get(ind));
        }
        return opUnion;
    }
}
