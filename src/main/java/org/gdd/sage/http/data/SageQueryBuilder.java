package org.gdd.sage.http.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;

/**
 * Builder used to create JSON queries thta can be send to a SaGe server
 * @author Thomas Minier
 */
public class SageQueryBuilder {

    private String next;
    private ObjectNode queryNode;
    private ObjectMapper mapper;

    private SageQueryBuilder() {
        mapper = new ObjectMapper();
        queryNode = mapper.createObjectNode();
    }

    /**
     * Get a builder used to create a new Sage JSON query
     * @return A SageQueryBuilder ready to be configured
     */
    public static SageQueryBuilder builder() {
        return new SageQueryBuilder();
    }

    /**
     * Set the Basic Graph Pattern of the query
     * @param patterns - The Basic Graph Patterns to be send with the query
     * @return The SageQueryBuilder instance, used for chaining calls
     */
    public SageQueryBuilder withBasicGraphPattern(BasicPattern patterns) {
        ArrayNode bgp = mapper.createArrayNode();
        for (Triple pattern: patterns.getList()) {
            ObjectNode jsonTriple = mapper.createObjectNode();
            jsonTriple.put("subject", pattern.getSubject().toString());
            jsonTriple.put("predicate", pattern.getPredicate().toString());
            jsonTriple.put("object", pattern.getObject().toString());
            bgp.add(jsonTriple);
        }
        queryNode.putArray("bgp").addAll(bgp);
        return this;
    }

    /**
     * Set the type, e.g., "bgp", of the query
     * @param type - The type of the query
     * @return The SageQueryBuilder instance, used for chaining calls
     */
    public SageQueryBuilder withType(String type) {
        queryNode.put("type", type);
        return this;
    }

    /**
     * Set the "next" field in the Sage query, i.e., a saved query execution plan to be resumed.
     * @param next - The next link
     * @return The SageQueryBuilder instance, used for chaining calls
     */
    public SageQueryBuilder withNextLink(String next) {
        this.next = next;
        return this;
    }

    /**
     * Build the JSON query
     * @return The JSON query, in string format
     */
    public String build() {
        ObjectNode jsonQuery = mapper.createObjectNode();
        jsonQuery.set("query", queryNode);
        jsonQuery.put("next", next);
        return jsonQuery.toString();
    }
}
