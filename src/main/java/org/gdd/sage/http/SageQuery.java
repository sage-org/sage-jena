package org.gdd.sage.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.jena.graph.Triple;

import java.util.List;

public class SageQuery {

    private String type = "bgp";
    private ArrayNode patterns;
    private String next;
    private ObjectMapper mapper;

    public SageQuery(List<Triple> patterns, String next) {
        mapper = new ObjectMapper();
        this.patterns = mapper.createArrayNode();
        for (Triple pattern: patterns) {
            ObjectNode jsonTriple = mapper.createObjectNode();
            jsonTriple.put("subject", pattern.getSubject().toString());
            jsonTriple.put("predicate", pattern.getPredicate().toString());
            jsonTriple.put("object", pattern.getObject().toString());
            this.patterns.add(jsonTriple);
        }
        this.next = next;
    }

    public String toJSONString() {
        ObjectNode queryNode = mapper.createObjectNode();
        queryNode.put("type", this.type);
        queryNode.putArray("bgp").addAll(this.patterns);

        ObjectNode jsonQuery = mapper.createObjectNode();
        jsonQuery.set("query", queryNode);
        jsonQuery.put("next", this.next);
        return jsonQuery.toString();
    }
}
