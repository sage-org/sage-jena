package org.gdd.sage.http;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SageResponse {
    public List<Map<String, String>> bindings;
    public int pageSize;
    public String next;
    public boolean hasNext;
}
