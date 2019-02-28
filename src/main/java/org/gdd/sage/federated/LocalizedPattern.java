package org.gdd.sage.federated;

import org.apache.jena.graph.Triple;

import java.util.Objects;

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

    public void setSource(String source) {
        this.source = source;
    }

    public Triple getPattern() {
        return pattern;
    }

    public void setPattern(Triple pattern) {
        this.pattern = pattern;
    }

    public int getCardinality() {
        return cardinality;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
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
        /*if (pattern.equals(o.getPattern())) {
            return source.compareTo(o.getSource());
        }
        return pattern.toString().compareTo(o.getPattern().toString());*/
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
