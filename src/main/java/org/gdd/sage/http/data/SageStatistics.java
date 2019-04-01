package org.gdd.sage.http.data;

import org.apache.jena.graph.Triple;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistics found in a page of results
 * @author Thomas Minier
 */
public class SageStatistics {
    private double suspendTime;
    private double resumeTime;
    private Map<String, Integer> cardinalities;

    public SageStatistics(double suspendTime, double resumeTime) {
        this.suspendTime = suspendTime;
        this.resumeTime = resumeTime;
        this.cardinalities = new HashMap<>();
    }

    public double getSuspendTime() {
        return suspendTime;
    }

    public double getResumeTime() {
        return resumeTime;
    }

    public void addTripleCardinality(String subject, String predicate, String object, int cardinality) {
        String key = buildTripleKey(subject, predicate, object);
        if (!cardinalities.containsKey(key)) {
            cardinalities.put(key, cardinality);
        }
    }

    public boolean hasTripleCardinality(String subject, String predicate, String object) {
        String key = buildTripleKey(subject, predicate, object);
        return cardinalities.containsKey(key);
    }

    public int getCardinality(Triple triple) {
        return getCardinality(triple.getSubject().toString(), triple.getPredicate().toString(), triple.getObject().toString());
    }

    public int getCardinality (String subject, String predicate, String object) {
        String key = buildTripleKey(subject, predicate, object);
        if (cardinalities.containsKey(key)) {
            return cardinalities.get(key);
        }
        return 0;
    }

    private String buildTripleKey(String subject, String predicate, String object) {
        return "s=" + subject + ";p=" + predicate + "o=" + object;
    }
}
