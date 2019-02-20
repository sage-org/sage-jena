package org.gdd.sage.federated;

import com.google.common.collect.Sets;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformBase;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.BasicPattern;
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

    /**
     * Test if a group of localized patterns is an exclusive group, i.e, the following conditions are met:
     *  - All patterns in the group are localized with the same RDF graph
     *  - Building this exclusive group will not generate any cartesian product
     * @param patterns - Group of triple patterns to test
     * @return True if the group is a valid exclusive group, False otherwise
     */
    private boolean isExclusiveGroup(List<LocalizedPattern> patterns) {
        if (patterns.isEmpty()) {
            return false;
        }
        String uri = patterns.get(0).getSource();
        for(int ind = 1; ind < patterns.size(); ind++) {
            if (!patterns.get(ind).getSource().equals(uri)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Build an OPService clause with a single triple pattern inside
     * @param graphURI - URI of the service clause
     * @param pattern - Triple pattern
     * @return The new OpService clause
     */
    private OpService getOpService(String graphURI, LocalizedPattern pattern) {
        BasicPattern bgp = new BasicPattern();
        bgp.add(pattern.getPattern());
        return new OpService(NodeFactory.createURI(graphURI), new OpBGP(bgp), false);
    }

    /**
     * Build an OPService clause with a set of triple patterns inside, i.e., a Basic graph pattern
     * @param graphURI - URI of the service clause
     * @param patterns - Set of triple patterns
     * @return The new OpService clause
     */
    private OpService getOpService(String graphURI, List<LocalizedPattern> patterns) {
        BasicPattern bgp = new BasicPattern();
        patterns.forEach(localizedPattern -> bgp.add(localizedPattern.getPattern()));
        return new OpService(NodeFactory.createURI(graphURI), new OpBGP(bgp), false);
    }

    @Override
    public Op transform(OpBGP opBGP) {
        List<Set<LocalizedPattern>> localizedPatterns = new LinkedList<>();

        // perform localization for each triple pattern and each source
        for(Triple pattern: opBGP.getPattern().getList()) {
            Set<LocalizedPattern> relevantSources = new HashSet<>();
            for(Map.Entry<String, SageRemoteClient> source: httpClients.entrySet()) {
                if(selectionStrategy.isRelevant(pattern, source.getKey(), source.getValue())) {
                    relevantSources.add(new LocalizedPattern(source.getKey(), pattern));
                }
            }
            // no relevant source => pattern has no matching RDF triples => bgp has no results
            if (relevantSources.isEmpty()) {
                return new OpBGP(new BasicPattern());
            }
            localizedPatterns.add(relevantSources);
        }

        // apply join distribution (P1 UNION P2) JOIN P3 => (P1 JOIN P3) UNION (P2 JOIN P3)
        List<Op> joins = new LinkedList<>();
        for(List<LocalizedPattern> joinGroup: Sets.cartesianProduct(localizedPatterns)) {
            Op opJoin;
            // if possible, build a cartesian product-free exclusive group
            if(isExclusiveGroup(joinGroup)) {
                opJoin = getOpService(joinGroup.get(0).getSource(), joinGroup);
            } else {
                LocalizedPattern p = joinGroup.get(0);
                opJoin = getOpService(p.getSource(), p);
                for(int ind = 1; ind < joinGroup.size(); ind++) {
                    p = joinGroup.get(ind);
                    opJoin = OpJoin.create(opJoin, getOpService(p.getSource(), p));
                }
            }
            joins.add(opJoin);
        }
        // build top-level union
        Op opUnion = joins.get(0);
        for(int ind = 1; ind < joins.size(); ind++) {
            opUnion = new OpUnion(opUnion, joins.get(ind));
        }
        return opUnion;
    }
}
