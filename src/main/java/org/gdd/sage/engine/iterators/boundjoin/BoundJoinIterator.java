package org.gdd.sage.engine.iterators.boundjoin;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.gdd.sage.engine.iterators.base.BlockBufferedIterator;
import org.gdd.sage.http.SageRemoteClient;

import java.util.*;

/**
 * An iterator which evaluates a Bound Join between an input iterator and a BGP
 * @author Thomas Minier
 */
public class BoundJoinIterator extends BlockBufferedIterator {
    private String graphURI;
    protected SageRemoteClient client;
    private BasicPattern bgp;

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
        return new BoundIterator(graphURI, client, bgpBucket, block, rewritingMap, isContainmentQuery);
    }
}
