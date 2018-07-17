package org.gdd.sage.engine.iterators.boundjoin;

import com.google.common.collect.Lists;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.gdd.sage.engine.iterators.base.BufferedIterator;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;
import org.slf4j.Logger;

import java.util.*;

/**
 * An iterator which evaluates a Bound Join between an input iterator and a BGP
 * @author Thomas Minier
 */
public class BoundJoinIterator extends BufferedIterator {
    protected SageRemoteClient client;
    protected Optional<String> nextLink;
    private BasicPattern bgp;
    protected boolean hasNextPage;
    private QueryIterator source;
    protected List<Binding> bindingBucket;
    protected List<BasicPattern> bgpBucket;
    private Map<Integer, Binding> rewritingMap;
    private int bucketSize;
    protected Logger logger;

    /**
     * Constructor
     * @param source - Input for the join
     * @param client - HTTP client used to query the SaGe server
     * @param bgp    - Basic Graph pattern to join with
     * @param bucketSize - Size of the bound join bucket (15 is the "default" admitted value)
     */
    public BoundJoinIterator(QueryIterator source, SageRemoteClient client, BasicPattern bgp, int bucketSize) {
        super();
        this.client = client;
        this.bgp = bgp;
        this.source = source;
        this.nextLink = Optional.empty();
        this.hasNextPage = false;
        this.bindingBucket = new ArrayList<>();
        this.bgpBucket = new ArrayList<>();
        this.rewritingMap = new HashMap<>();
        this.bucketSize = bucketSize;
        logger = ARQ.getExecLogger();
    }

    @Override
    protected boolean canProduceBindings() {
        try {
            return source.hasNext() || hasNextPage;
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    /**
     * Find a rewriting key in a list of variables
     * For example, in [ ?s, ?o_1 ], the rewriting key is 1
     * @param vars List of SPARQL variables to analyze
     * @return The rewriting key found, or -1 if no key was found
     */
    private int findKey(List<Var> vars) {
        int key = -1;
        for(Var v: vars) {
            for(int k = 1; k <= bucketSize; k++) {
                if (v.getVarName().endsWith("_" + k)) {
                    return k;
                }
            }
        }
        return key;
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
     * Undo the bound join rewriting on solutions bindings, e.g., rewrite all variables "?o_1" to "?o"
     * @param key Rewriting key used
     * @param input Binding to process
     * @param vars List of the binding variables
     * @return
     */
    private BindingHashMap revertBinding (int key, Binding input, List<Var> vars) {
        BindingHashMap newBinding = new BindingHashMap();
        for(Var v: vars) {
            String vName = v.getVarName();
            if (vName.endsWith("_" + key)) {
                int index = vName.indexOf("_" + key);
                newBinding.add(Var.alloc(vName.substring(0, index)), input.get(v));
            } else {
                newBinding.add(v, input.get(v));
            }
        }
        return newBinding;
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

    /**
     * Undo the rewriting on solutions bindings, and then merge each of them with the corresponding input binding
     * @param input Solutions bindings to process
     * @return
     */
    protected List<Binding> rewriteSolutions(List<Binding> input) {
        List<Binding> solutions = new LinkedList<>();
        if (!input.isEmpty()) {
            for(Binding oldBinding: input) {
                List<Var> vars = Lists.newArrayList(oldBinding.vars());
                int key = findKey(vars);
                // rewrite binding, and then merge it with the corresponding one in the bucket
                BindingHashMap newBinding = revertBinding(key, oldBinding, vars);
                if (rewritingMap.containsKey(key)) {
                    newBinding.addAll(rewritingMap.get(key));
                }
                solutions.add(newBinding);
            }
        }
        System.out.println(solutions);
        return solutions;
    }

    @Override
    protected List<Binding> produceBindings() {
        List<Binding> solutions = new ArrayList<>();
        // if no next page, try to read from source to build a buffer of bounded BGps
        if (!nextLink.isPresent()) {
            bindingBucket.clear();
            bgpBucket.clear();
            rewritingMap.clear();
            try {
                while (source.hasNext() && bgpBucket.size() < bucketSize) {
                    Binding b = source.next();
                    BasicPattern boundedBGP = new BasicPattern();
                    // key used for the rewriting
                    int key = bgpBucket.size() + 1;
                    for (Triple t: bgp) {
                        Triple boundedTriple = Substitute.substitute(t, b);
                        // perform rewriting and register it
                        boundedTriple = rewriteTriple(key, boundedTriple);
                        rewritingMap.put(key, b);
                        // add rewritten triple to BGP
                        boundedBGP.add(boundedTriple);
                    }
                    bgpBucket.add(boundedBGP);
                    bindingBucket.add(b);
                }
            } catch (NoSuchElementException e) {
                // silently do nothing
            }
        }

        if (!bgpBucket.isEmpty()) {
            // send union query to sage server
            QueryResults queryResults = client.query(bgpBucket, nextLink);
            if (queryResults.hasError()) {
                // an error has occurred, report it
                hasNextPage = false;
                logger.error(queryResults.getError());
            } else {
                solutions.addAll(rewriteSolutions(reviewResults(queryResults)));
            }
        }
        return solutions;
    }
}
