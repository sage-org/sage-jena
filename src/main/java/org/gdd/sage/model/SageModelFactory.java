package org.gdd.sage.model;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

public class SageModelFactory {

    public static final Model createModel(String url) {
        return ModelFactory.createModelForGraph(new SageGraph(url));
    }
}
