package org.gdd.sage.http.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SageResponse {
    public List<Map<String, String>> bindings;
    public int pageSize;
    public String next;
    public boolean hasNext;
    @JsonDeserialize(using = SageStatisticsDeserializer.class)
    public SageStatistics stats;
}
