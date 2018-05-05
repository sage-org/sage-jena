package org.gdd.sage.http;

import org.apache.jena.sparql.core.BasicPattern;
import org.gdd.sage.http.data.QueryResults;

import java.util.Optional;
import java.util.concurrent.Future;

public interface SageRemoteClient {
    Future<QueryResults> query(BasicPattern bgp);
    Future<QueryResults> query(BasicPattern bgp, Optional<String> next);
}
