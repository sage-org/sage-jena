package org.gdd.sage.http;

import org.apache.jena.sparql.core.BasicPattern;

import java.io.IOException;

public interface SageRemoteClient {
    QueryResults query(BasicPattern bgp) throws IOException;
    QueryResults query(BasicPattern bgp, String next) throws IOException;
}
