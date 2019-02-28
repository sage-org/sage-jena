package org.gdd.sage.http.data;

import org.apache.jena.graph.Triple;

import java.util.HashMap;
import java.util.Map;

/**
 * Statistics found in a page of results
 * @author Thomas Minier
 */
public class SageStatistics {
    private double exportTime;
    private double importTime;
    private Map<String, Integer> cardinalities;

    public SageStatistics(double exportTime, double importTime) {
        this.exportTime = exportTime;
        this.importTime = importTime;
        this.cardinalities = new HashMap<>();
    }

    public double getExportTime() {
        return exportTime;
    }

    public double getImportTime() {
        return importTime;
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
