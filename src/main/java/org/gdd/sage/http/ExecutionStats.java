package org.gdd.sage.http;

import org.apache.jena.ext.com.google.common.math.Stats;

import java.util.ArrayList;
import java.util.List;

/**
 * An utility class used to record statistic during query execution
 * @author Thomas Minier
 */
public class ExecutionStats {
    private double executionTime;
    private int nbCalls;
    private List<Double> httpTimes;
    private List<Double> importTimes;
    private List<Double> exportTimes;

    public ExecutionStats() {
        executionTime = -1;
        nbCalls = 0;
        httpTimes = new ArrayList<>();
        importTimes = new ArrayList<>();
        exportTimes = new ArrayList<>();
    }

    public double getExecutionTime() {
        return executionTime;
    }

    public int getNbCalls() {
        return nbCalls;
    }

    public Double getMeanHttpTimes() {
        if (httpTimes.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(httpTimes);
    }

    public Double getMeanImportTimes() {
        if (importTimes.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(importTimes);
    }

    public Double getMeanExportTimes() {
        if (exportTimes.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(exportTimes);
    }

    public void startTimer() {
        executionTime = System.nanoTime();
    }

    public void stopTimer() {
        double endTime = System.nanoTime();
        executionTime = (endTime - executionTime) / 1e9;
    }

    public void reportHttpQuery(double execTime) {
        nbCalls++;
        httpTimes.add(execTime);
    }

    public void reportOverhead(double importTime, double exportTime) {
        importTimes.add(importTime);
        exportTimes.add(exportTime);
    }
}
