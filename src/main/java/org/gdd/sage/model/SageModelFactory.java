package org.gdd.sage.model;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

/**
 * Helper factory used to create Jena Models connected to a remote SaGe server
 */
public class SageModelFactory {
    public static Model createModel(String url) {
        return ModelFactory.createModelForGraph(new SageGraph(url));
    }
}
