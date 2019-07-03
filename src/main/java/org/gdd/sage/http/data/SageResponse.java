package org.gdd.sage.http.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SageResponse {
    @JsonDeserialize(using = BindingsDeserializer.class)
    public QuerySolutions bindings;
    public int pageSize;
    public String next;
    public boolean hasNext;
    @JsonDeserialize(using = SageStatisticsDeserializer.class)
    public SageStatistics stats;
}
