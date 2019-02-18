package org.gdd.sage.engine.iterators.boundjoin;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.engine.iterators.base.BlockBufferedIterator;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;
import org.slf4j.Logger;

import java.util.*;

/**
 * An iterator which evaluates a Bound Join between an input iterator and a BGP
 * @author Thomas Minier
 */
public class BoundJoinIterator extends BlockBufferedIterator {
    private String graphURI;
    protected SageRemoteClient client;
    protected Optional<String> nextLink;
    private BasicPattern bgp;
    protected boolean hasNextPage;
    private Logger logger;

    /**
     * Constructor
     * @param source - Input for the join
     * @param client - HTTP client used to query the SaGe server
     * @param bgp    - Basic Graph pattern to join with
     * @param bucketSize - Size of the bound join bucket (15 is the "default" admitted value)
     */
    public BoundJoinIterator(QueryIterator source, String graphURI, SageRemoteClient client, BasicPattern bgp, int bucketSize, ExecutionContext context) {
        super(source, bucketSize, context);
        this.graphURI = graphURI;
        this.client = client;
        this.bgp = bgp;
        this.nextLink = Optional.empty();
        logger = ARQ.getExecLogger();
    }

    /**
     * Rewrite a triple pattern using a rewriting key, i.e., append "_key" to each SPARQL variable in the triple pattern
     * @param key Rewriting key
     * @param tp Triple pattern to rewrite
     * @return The rewritten triple pattern
     */
    private Triple rewriteTriple (int key, Triple tp) {
        Node subj = tp.getSubject();
        Node pred = tp.getPredicate();
        Node obj = tp.getObject();
        if (subj.isVariable()) {
            subj = NodeFactory.createVariable(subj.getName() + "_" + key);
        }
        if (pred.isVariable()) {
            pred = NodeFactory.createVariable(pred.getName() + "_" + key);
        }
        if (obj.isVariable()) {
            obj = NodeFactory.createVariable(obj.getName() + "_" + key);
        }
        return new Triple(subj, pred, obj);
    }


    /**
     * Do something with bound join query results
     * @param results - Query results fetched from the Sage server
     */
    private List<Binding> reviewResults(QueryResults results) {
        List<Binding> solutions = results.getBindings();
        if (!solutions.isEmpty()) {
            nextLink = results.getNext();
            hasNextPage = results.hasNext();
        }
        return solutions;
    }

    protected QueryIterator createIterator(List<BasicPattern> bag, List<Binding> block, Map<Integer, Binding> rewritingMap, boolean isContainmentQuery) {
        return new BoundIterator(graphURI, client, bag, block, rewritingMap, isContainmentQuery);
    }


    @Override
    protected QueryIterator processBlock(List<Binding> block) {
        // data structures used for bind join
        List<BasicPattern> bgpBucket = new LinkedList<>();
        Map<Integer, Binding> rewritingMap = new HashMap<>();

        boolean isContainmentQuery = false;

        // key used for the rewriting
        int key = 0;
        for(Binding b: block) {
            BasicPattern boundedBGP = new BasicPattern();
            for (Triple t: bgp) {
                Triple boundedTriple = Substitute.substitute(t, b);
                isContainmentQuery = (!boundedTriple.getSubject().isVariable()) && (!boundedTriple.getPredicate().isVariable()) && (!boundedTriple.getObject().isVariable());
                // perform rewriting and register it
                boundedTriple = rewriteTriple(key, boundedTriple);
                rewritingMap.put(key, b);
                // add rewritten triple to BGP
                boundedBGP.add(boundedTriple);
            }
            bgpBucket.add(boundedBGP);
            key++;
        }
        return createIterator(bgpBucket, block, rewritingMap, isContainmentQuery);
    }
}
