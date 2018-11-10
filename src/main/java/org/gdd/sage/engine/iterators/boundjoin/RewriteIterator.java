package org.gdd.sage.engine.iterators.boundjoin;

import com.google.common.collect.Lists;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.gdd.sage.engine.iterators.base.BufferedIterator;
import org.gdd.sage.http.SageRemoteClient;
import org.gdd.sage.http.data.QueryResults;
import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RewriteIterator extends BufferedIterator {
    private SageRemoteClient client;
    protected Optional<String> nextLink;
    protected boolean hasNextPage;
    private List<BasicPattern> bag;
    private List<Binding> block;
    private int bagSize;
    private Map<Integer, Binding> rewritingMap;
    private boolean isContainmentQuery;

    private Logger logger;

    public RewriteIterator(SageRemoteClient client, List<BasicPattern> bag, List<Binding> block, Map<Integer, Binding> rewritingMap, boolean isContainmentQuery) {
        this.client = client;
        this.nextLink = Optional.empty();
        this.bag = bag;
        this.block = block;
        this.bagSize = bag.size();
        this.rewritingMap = rewritingMap;
        hasNextPage = true;
        this.isContainmentQuery = isContainmentQuery;
        logger = ARQ.getExecLogger();
    }

    public List<Binding> getBlock() {
        return block;
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
            for(int k = 0; k <= bagSize; k++) {
                if (v.getVarName().endsWith("_" + k)) {
                    return k;
                }
            }
        }
        return key;
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
     * Undo the rewriting on solutions bindings, and then merge each of them with the corresponding input binding
     * @param input Solutions bindings to process
     * @return
     */
    protected List<Binding> rewriteSolutions(List<Binding> input) {
        List<Binding> solutions = new LinkedList<>();
        if (input.size() == 1 && input.get(0).isEmpty()) {
            input.clear();
        }
        if (input.isEmpty() && isContainmentQuery) {
            solutions.addAll(block);
        } else if (!input.isEmpty()) {
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
        return solutions;
    }

    /**
     * Do something with bound join query results
     * @param results - Query results fetched from the Sage server
     */
    private List<Binding> reviewResults(QueryResults results) {
        List<Binding> solutions = results.getBindings();
        nextLink = results.getNext();
        hasNextPage = results.hasNext();
        return solutions;
    }

    @Override
    protected boolean canProduceBindings() {
        return hasNextPage;
    }

    @Override
    protected List<Binding> produceBindings() {
        List<Binding> solutions = new LinkedList<>();
        QueryResults queryResults = client.query(bag, nextLink);
        if (queryResults.hasError()) {
            // an error has occurred, report it
            hasNextPage = false;
            logger.error(queryResults.getError());
        } else {
            solutions.addAll(rewriteSolutions(reviewResults(queryResults)));
        }
        return solutions;
    }
}
