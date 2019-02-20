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
    private Map<Triple, Integer> cardinalities;

    public double getExportTime() {
        return exportTime;
    }

    public double getImportTime() {
        return importTime;
    }

    public SageStatistics(double exportTime, double importTime) {
        this.exportTime = exportTime;
        this.importTime = importTime;
        this.cardinalities = new HashMap<>();
    }

    public void addTriple(String subj, String pred, String obj, int cardinality) {
        //Triple triple = new Triple(NodeFactoryExtra.parseNode(subj), NodeFactoryExtra.parseNode(pred), NodeFactoryExtra.parseNode(obj));
        //cardinalities.put(triple, cardinality);
    }

    public int getCardinality(Triple triple) {
        return cardinalities.get(triple);
    }
}
