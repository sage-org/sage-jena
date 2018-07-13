package org.gdd.sage.engine;

import org.apache.jena.sparql.util.Symbol;

/**
 * Symbols used by the SageQuery to tag metadata in Jena Execution contexts
 * @author Thomas Minier
 */
public class SageSymbols {

    private SageSymbols() {}

    /**
     * Symbol used to indicate an OPTIONAL clause
     */
    public static final Symbol OPTIONAL_SYMBOL = Symbol.create("optional");
}
