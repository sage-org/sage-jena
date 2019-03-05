package org.gdd.sage.federated;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpService;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.gdd.sage.core.SageUtils;

import java.util.*;

/**
 * An exclusive group, as defined in FedX research article.
 * It represents a set of triple pattern that can be evaluated at the same source (a RDF Graph URI).
 * @author Thomas Minier
 */
public class ExclusiveGroup {
    private String graphURL;
    private BasicPattern bgp;

    public ExclusiveGroup(String graphURL) {
        this.graphURL = graphURL;
        this.bgp = new BasicPattern();
    }

    public ExclusiveGroup(LocalizedPattern pattern) {
        this.graphURL = pattern.getSource();
        this.bgp = new BasicPattern();
        this.bgp.add(pattern.getPattern());
    }

    public ExclusiveGroup(List<LocalizedPattern> patterns) {
        this.graphURL = patterns.get(0).getSource();
        this.bgp = new BasicPattern();
        patterns.forEach(localizedPattern -> this.bgp.add(localizedPattern.getPattern()));
    }

    public String getGraphURL() {
        return graphURL;
    }

    public BasicPattern getBgp() {
        return bgp;
    }

    /**
     * Add a localized triple pattern to the exclusive group
     * @param pattern - Triple pattern to add
     */
    public void addPattern(LocalizedPattern pattern) {
        bgp.add(pattern.getPattern());
    }

    /**
     * Get the number of triple patterns in the exclusive group
     * @return The number of triple patterns in the exclusive group
     */
    public int size() {
        return bgp.size();
    }

    /**
     * Test if the exclusive group is empty, i.e., it does not contains any triple patterns
     * @return True if the exclusive group is empty, False otherwise
     */
    public boolean isEmpty() {
        return bgp.isEmpty();
    }

    /**
     * Get all SPARQL variables from all triple patterns in the group
     * @return All SPARQL variables from all triple patterns in the group
     */
    public Set<Var> getVariables() {
        Set<Var> res = new HashSet<>();
        for(Triple pattern: bgp.getList()) {
            res.addAll(SageUtils.getVariables(pattern));
        }
        return res;
    }

    /**
     * Transform the exclusive group into a SPARQL logical node
     * @return The equivalent SPARQL logical node
     */
    public Op toOp() {
        Op opBGP = new OpBGP(bgp);
        return new OpService(NodeFactory.createURI(graphURL), opBGP, false);
    }

    @Override
    public String toString() {
        return "ExclusiveGroup{" +
                "graphURL='" + graphURL + '\'' +
                ", bgp=" + bgp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExclusiveGroup that = (ExclusiveGroup) o;
        return getGraphURL().equals(that.getGraphURL()) &&
                getBgp().equals(that.getBgp());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getGraphURL(), getBgp());
    }
}
