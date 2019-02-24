package org.gdd.sage.engine;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.QC;
import org.gdd.sage.core.SageUtils;
import org.gdd.sage.engine.iterators.optional.OptJoin;
import org.gdd.sage.engine.iterators.optional.OptionalIterator;
import org.gdd.sage.engine.iterators.parallel.ParallelUnionIterator;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * An OpExecutor that executes SPARQL query with the Sage smart client.
 * @author Thomas Minier
 */
public class SageOpExecutor extends OpExecutor {
    private final ExecutorService threadPool;

    SageOpExecutor(ExecutorService threadPool, ExecutionContext execCxt) {
        super(execCxt);
        this.threadPool = threadPool;
    }

    @Override
    protected QueryIterator exec(Op op, QueryIterator input) {
        return super.exec(op, input);
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
                leftVariables.addAll(SageUtils.getVariables(pattern));
            }
            // SPARQL variables in P_1 JOIN P_2
            Set<Var> joinVariables = new HashSet<>(leftVariables);
            for(Triple pattern: rightBGP.getList()) {
                joinVariables.addAll(SageUtils.getVariables(pattern));
            }

            // build iterators to evaluate the OptJoin
            QueryIterator leftIterator = QC.execute(new OpBGP(join), input, execCxt);
            QueryIterator rightIterator = QC.execute(new OpBGP(leftBGP), input, execCxt);
            QueryIterator source = new ParallelUnionIterator(threadPool, leftIterator, rightIterator);
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
            throw new ARQInternalErrorException("The RDF Dataset does not contains the named graph <" + graphNode + ">");
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

    @Override
    protected QueryIterator execute(OpUnion opUnion, QueryIterator input) {
        if (input.isJoinIdentity()) {
            QueryIterator leftIterator = QC.execute(opUnion.getLeft(), input, execCxt);
            QueryIterator rightIterator = QC.execute(opUnion.getRight(), input, execCxt);
            return new ParallelUnionIterator(threadPool, leftIterator, rightIterator);
        }
        return super.execute(opUnion, input);
    }

    /*@Override
    protected QueryIterator execute(OpFilter opFilter, QueryIterator input) {
        QueryIterator qIter;
        ExprList filters = opFilter.getExprs();
        // TODO for now, we limit filter packing fpr the first BGP in the plan
        if (opFilter.getSubOp() instanceof OpBGP && input.isJoinIdentity() && stageGenerator instanceof SageStageGenerator) {
            BasicPattern bgp = ((OpBGP) opFilter.getSubOp()).getPattern();
            SageStageGenerator sageStageGenerator = (SageStageGenerator) stageGenerator;
            qIter = sageStageGenerator.execute(bgp, input, execCxt, filters);
        } else {
            qIter = super.exec(opFilter.getSubOp(), input);
        }
        // add all filters, for safety purpose
        for(Expr expr: filters.getList()) {
            qIter = new QueryIterFilterExpr(qIter, expr, execCxt);
        }
        return qIter;
    }*/
}
