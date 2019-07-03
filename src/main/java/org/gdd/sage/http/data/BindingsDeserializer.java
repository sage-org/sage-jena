package org.gdd.sage.http.data;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deserialize bindings from JSON to Jena format
 * @author Thomas Minier
 */
public class BindingsDeserializer extends JsonDeserializer<QuerySolutions> {
    private final static Pattern TYPE_PATTERN = Pattern.compile("\"(.*)\"(\\^\\^)(.+)");
    private final static Pattern LANG_PATTERN = Pattern.compile("\"(.*)\"(@)(.+)");

    @Override
    public QuerySolutions deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        QuerySolutions res = new QuerySolutions();
        ArrayNode node = p.getCodec().readTree(p);
        // for each set of bindings
        node.forEach(jsonNode -> {
            BindingHashMap bindings = new BindingHashMap();
            List<Binding> group = new LinkedList<>();

            // for each binding
            jsonNode.fieldNames().forEachRemaining(s -> {
                // if it is a group of bindings
                if (s.equals("?__group_values")) {
                    ArrayNode g = (ArrayNode) jsonNode.get(s);
                    // for each element of the group
                    g.forEach(jnode -> {
                        BindingHashMap elt = new BindingHashMap();
                        jnode.fields().forEachRemaining(entry -> {
                            elt.add(Var.alloc(entry.getKey().substring(1)), parseNode(entry.getValue().asText()));
                        });
                        group.add(elt);
                    });
                } else {
                    // otherwise it's a regular binding
                    bindings.add(Var.alloc(s.substring(1)), parseNode(jsonNode.get(s).asText()));
                }
            });
            // no group found => all bindings found are solution bindings
            if (group.isEmpty()) {
                res.addBindings(bindings);
            } else {
                // otherwise, register the new group
                SolutionGroup solutionGroup = new SolutionGroup();
                group.forEach(solutionGroup::addBindings);
                // all bindings found inside the set of solutions are the group keys
                // TODO what about aggregates ? count, etc need to see that
                bindings.vars().forEachRemaining(var -> {
                    solutionGroup.addKey(var, bindings.get(var));
                });
                res.addSolutionGroup(solutionGroup);
            }
        });
        return res;
    }

    /**
     * Parse a RDF node from String format to a Jena compatible format
     * @param node RDF node in string format
     * @return RDF node in a Jena compatible format
     */
    private static Node parseNode(String node) {
        Node value = null;
        try {
            String literal;
            try {
                // Literal case
                if (node.startsWith("\""))  {
                    literal = node.trim();
                    Matcher langMatcher = LANG_PATTERN.matcher(literal);
                    Matcher typeMatcher = TYPE_PATTERN.matcher(literal);
                    langMatcher.matches();
                    typeMatcher.matches();
                    if (typeMatcher.matches()) {
                        if (typeMatcher.group(3).startsWith("<")) {
                            String type = typeMatcher.group(3);
                            RDFDatatype datatype = TypeMapper.getInstance().getTypeByName(type.substring(1, type.length() - 1));
                            value = NodeFactory.createLiteral(typeMatcher.group(1), datatype);
                        } else {
                            RDFDatatype datatype = TypeMapper.getInstance().getTypeByName(typeMatcher.group(3));
                            value = NodeFactory.createLiteral(typeMatcher.group(1), datatype);
                        }
                    } else if (langMatcher.matches()) {
                        value = NodeFactory.createLiteral(langMatcher.group(1), langMatcher.group(3));
                    } else if (literal.startsWith("\"") && literal.endsWith("\"")){
                        value = NodeFactory.createLiteral(literal.substring(1, literal.length() - 1));
                    } else {
                        value = NodeFactory.createLiteral(literal);
                    }
                } else {
                    value = NodeFactory.createURI(node);
                }
            } catch(Exception e) {
                e.printStackTrace();
                throw e;
            }
        } catch(Exception e) {
            // TODO: for now we skip parsing errors, maybe need to do something cleaner
            System.err.println(node);
        }
        return value;
    }
}
