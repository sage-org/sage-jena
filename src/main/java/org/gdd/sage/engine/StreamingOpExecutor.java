package org.gdd.sage.engine;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.QC;
import org.gdd.sage.engine.iterators.base.UnionIterator;
import org.gdd.sage.engine.iterators.optional.OptJoin;
import org.gdd.sage.engine.iterators.optional.OptionalIterator;

import java.util.HashSet;
import java.util.Set;

/**
 * An OpExecutor that streams intermediate results for OpGraph and OpService operators,
 * and use a Bind join approach to evaluate Optionals.
 *
 * The default ARQ implementation perform join "binding per binding" using QueryIterSingleton to wrap
 * intermediate results. While this approach performs great when using Nested Loop Joins, joins
 * algorithms like Symmetric hash join, that require streaming inputs, cannot be applied.
 * @author Thomas Minier
 */
public class StreamingOpExecutor extends OpExecutor {

    StreamingOpExecutor(ExecutionContext execCxt) {
        super(execCxt);
    }

    @Override
    protected QueryIterator exec(Op op, QueryIterator input) {
        return super.exec(op, input);
    }

    /**
     * Get the set of SPARQL variables in a triple pattern
     * @param pattern - Triple pattern to analyze
     * @return The set of SPARQL variables in the triple pattern
     */
    private Set<Var> getVariables(Triple pattern) {
        Set<Var> res = new HashSet<>();
        if(pattern.getSubject().isVariable() && !pattern.getSubject().toString().startsWith("??")) {
            res.add((Var) pattern.getSubject());
        }
        if(pattern.getPredicate().isVariable() && pattern.getPredicate().toString().startsWith("??")) {
            res.add((Var) pattern.getPredicate());
        }
        if(pattern.getObject().isVariable() && !pattern.getObject().toString().startsWith("??")) {
            res.add((Var) pattern.getObject());
        }
        return res;
    }

    /**
     * Exxecute a Left Join/Optional with a pure pipeline logic
     * @param left - Left operator
     * @param right - Right operator
     * @param input - Solution bindings input
     * @return A QueryIterator that execute the left join
     */
    private QueryIterator executeOptional(Op left, Op right, QueryIterator input) {
        // use an optimized OptJoin if both operators are simple BGPs
        if (input.isJoinIdentity() && left instanceof OpBGP && right instanceof OpBGP) {
            // P_1
            BasicPattern leftBGP = ((OpBGP) left).getPattern();
            // P_2
            BasicPattern rightBGP = ((OpBGP) right).getPattern();
            // P_1 JOIN P_2
            BasicPattern join = new BasicPattern(leftBGP);
            join.addAll(rightBGP);

            // SPARQL variables in P_1
            Set<Var> leftVariables = new HashSet<>();
            for(Triple pattern: leftBGP.getList()) {
                leftVariables.addAll(getVariables(pattern));
            }
            // SPARQL variables in P_1 JOIN P_2
            Set<Var> joinVariables = new HashSet<>(leftVariables);
            for(Triple pattern: rightBGP.getList()) {
                joinVariables.addAll(getVariables(pattern));
            }

            // build iterators to evaluate the OptJoin
            QueryIterator leftSource = QC.execute(new OpBGP(join), input, execCxt);
            QueryIterator rightSource = QC.execute(new OpBGP(leftBGP), input, execCxt);
            QueryIterator source = new UnionIterator(leftSource, rightSource);
            return new OptJoin(source, leftVariables, joinVariables);
        }
        // otherwise, use a regular OptionalIterator
        QueryIterator leftIter = QC.execute(left, input, execCxt);
        return OptionalIterator.create(leftIter, right, execCxt);
    }

    @Override
    protected QueryIterator execute(OpService opService, QueryIterator input) {
        DatasetGraph dataset = execCxt.getDataset();
        Node graphNode = opService.getService();
        if ((!graphNode.isURI()) && (!graphNode.isBlank())) {
            throw new ARQInternalErrorException("Unexpected SERVICE node (not an URI) when evaluating SERVICE clause.\n" + graphNode);
        } else if (Quad.isDefaultGraph(graphNode)) {
            return exec(opService.getSubOp(), input);
        } else if (!dataset.containsGraph(graphNode)) {
            throw new ARQInternalErrorException("Dataset does not contains the named graph <" + graphNode + ">");
        }
        Graph currentGraph = dataset.getGraph(graphNode);
        ExecutionContext graphContext = new ExecutionContext(execCxt, currentGraph);
        return QC.execute(opService.getSubOp(), input, graphContext);
    }

    @Override
    protected QueryIterator execute(OpConditional opCondition, QueryIterator input) {
        return executeOptional(opCondition.getLeft(), opCondition.getRight(), input);
    }

    @Override
    protected QueryIterator execute(OpLeftJoin opLeftJoin, QueryIterator input) {
        return executeOptional(opLeftJoin.getLeft(), opLeftJoin.getRight(), input);
    }
}
