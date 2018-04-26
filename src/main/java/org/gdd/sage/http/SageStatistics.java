package org.gdd.sage.http;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SageStatistics {
    public Map<String, Map<String, Integer>> cardinalities;
}
