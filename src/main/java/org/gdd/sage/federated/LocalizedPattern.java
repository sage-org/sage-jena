package org.gdd.sage.federated;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.gdd.sage.core.SageUtils;

import java.util.Objects;
import java.util.Set;

/**
 * A Triple pattern localized with a RDF graph URI.
 * @author Thomas Minier
 */
class LocalizedPattern implements Comparable<LocalizedPattern> {
    private String source;
    private Triple pattern;
    private int cardinality;

    public LocalizedPattern(String source, Triple pattern, int cardinality) {
        this.source = source;
        this.pattern = pattern;
        this.cardinality = cardinality;
    }

    public String getSource() {
        return source;
    }

    public Triple getPattern() {
        return pattern;
    }

    public int getCardinality() {
        return cardinality;
    }

    /**
     * Get all SPARQL variables in the pattern
     * @return All SPARQL variables in the pattern
     */
    public Set<Var> getVariables() {
        return SageUtils.getVariables(pattern);
    }

    /**
     * Transform the localized pattern into a SPARQL logical node
     * @return The equivalent SPARQL logical node
     */
    public Op toOp() {
        BasicPattern bgp = new BasicPattern();
        bgp.add(pattern);
        Op opBGP = new OpBGP(bgp);
        return new OpService(NodeFactory.createURI(source), opBGP, false);
    }

    @Override
    public String toString() {
        return "LocalizedPattern{" +
                "source='" + source + '\'' +
                ", pattern=" + pattern +
                ", cardinality=" + cardinality +
                '}';
    }

    @Override
    public int compareTo(LocalizedPattern o) {
        return o.getCardinality() - getCardinality();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalizedPattern that = (LocalizedPattern) o;
        return getCardinality() == that.getCardinality() &&
                getSource().equals(that.getSource()) &&
                getPattern().equals(that.getPattern());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSource(), getPattern(), getCardinality());
    }
}
