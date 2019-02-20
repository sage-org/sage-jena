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

    public LocalizedPattern(String source, Triple pattern) {
        this.source = source;
        this.pattern = pattern;
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

    @Override
    public String toString() {
        return "LocalizedPattern{" +
                "source=" + source +
                ", pattern=" + pattern +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalizedPattern pattern1 = (LocalizedPattern) o;
        return source.equals(pattern1.source) &&
                pattern.equals(pattern1.pattern);
    }

    @Override
    public int compareTo(LocalizedPattern o) {
        if (pattern.equals(o.getPattern())) {
            return source.compareTo(o.getSource());
        }
        return pattern.toString().compareTo(o.getPattern().toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, pattern);
    }
}
