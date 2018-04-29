package org.gdd.sage.http.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.jena.graph.Triple;

import java.util.List;

public class SageQuery {

    public static String toJSONString(String type, List<Triple> patterns, String next) {
        ObjectMapper mapper = new ObjectMapper();

        ArrayNode bgp = mapper.createArrayNode();
        for (Triple pattern: patterns) {
            ObjectNode jsonTriple = mapper.createObjectNode();
            jsonTriple.put("subject", pattern.getSubject().toString());
            jsonTriple.put("predicate", pattern.getPredicate().toString());
            jsonTriple.put("object", pattern.getObject().toString());
            bgp.add(jsonTriple);
        }

        ObjectNode queryNode = mapper.createObjectNode();
        queryNode.put("type", type);
        queryNode.putArray("bgp").addAll(bgp);

        ObjectNode jsonQuery = mapper.createObjectNode();
        jsonQuery.set("query", queryNode);
        jsonQuery.put("next", next);
        return jsonQuery.toString();
    }
}
