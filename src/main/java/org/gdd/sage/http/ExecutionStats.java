package org.gdd.sage.http;

import org.apache.jena.ext.com.google.common.math.Stats;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * An utility class used to record statistic during query execution
 * @author Thomas Minier
 */
public class ExecutionStats {
    private double executionTime;
    private int nbCallsRead;
    private int nbCallsWrite;
    private List<Double> httpTimesRead;
    private List<Double> httpTimesWrite;
    private List<Double> resumeTimesRead;
    private List<Double> suspendTimesRead;
    private List<Double> resumeTimesWrite;
    private List<Double> suspendTimesWrite;

    public ExecutionStats() {
        executionTime = -1;
        nbCallsRead = 0;
        nbCallsWrite = 0;
        httpTimesRead = new ArrayList<>();
        httpTimesWrite = new ArrayList<>();
        resumeTimesRead = new ArrayList<>();
        suspendTimesRead = new ArrayList<>();
        resumeTimesWrite = new LinkedList<>();
        suspendTimesWrite = new LinkedList<>();
    }

    public double getExecutionTime() {
        return executionTime;
    }

    public int getNbCallsRead() {
        return nbCallsRead;
    }

    public int getNbCallsWrite() {
        return nbCallsWrite;
    }

    public Double getMeanHTTPTimesRead() {
        if (httpTimesRead.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(httpTimesRead);
    }

    public Double getMeanHTTPTimesWrite() {
        if (httpTimesWrite.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(httpTimesWrite);
    }

    public Double getMeanResumeTimeRead() {
        if (resumeTimesRead.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(resumeTimesRead);
    }

    public Double getMeanSuspendTimeRead() {
        if (suspendTimesRead.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(suspendTimesRead);
    }

    public Double getMeanResumeTimeWrite() {
        if (resumeTimesWrite.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(resumeTimesWrite);
    }

    public Double getMeanSuspendTimeWrite() {
        if (suspendTimesWrite.isEmpty()) {
            return 0.0;
        }
        return Stats.meanOf(suspendTimesWrite);
    }

    public void startTimer() {
        executionTime = System.nanoTime();
    }

    public void stopTimer() {
        double endTime = System.nanoTime();
        executionTime = (endTime - executionTime) / 1e9;
    }

    public void reportHTTPQueryRead(double execTime) {
        nbCallsRead++;
        httpTimesRead.add(execTime);
    }

    public void reportHTTPQueryWrite(double execTime) {
        nbCallsWrite++;
        httpTimesWrite.add(execTime);
    }

    public void reportOverheadRead(double resumeTime, double suspendTime) {
        resumeTimesRead.add(resumeTime);
        suspendTimesRead.add(suspendTime);
    }

    public void reportOverheadWrite(double resumeTime, double suspendTime) {
        resumeTimesWrite.add(resumeTime);
        suspendTimesWrite.add(suspendTime);
    }
}
