package org.gdd.sage.http;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * Builder used to create instances of SageRemoteClient
 * @author Thomas Minier
 */
public class SageClientBuilder {
    private String url;
    private HttpClient httpClient;

    private SageClientBuilder() {}

    public void setUrl(String url) {
        this.url = url;
    }

    public void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public static SageRemoteClient createDefault(String url) {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        CacheConfig cacheConfig = CacheConfig.custom()
                .setMaxCacheEntries(1000)
                .setMaxObjectSize(8192)
                .build();
        HttpClient client = CachingHttpClients.custom()
                .setCacheConfig(cacheConfig)
                .setConnectionManager(connectionManager)
                .build();
        return SageClientBuilder.create()
                .withURL(url)
                .withHTTPClient(client)
                .build();
    }

    public static SageClientBuilder create() {
        return new SageClientBuilder();
    }

    public SageClientBuilder withURL(String url) {
        setUrl(url);
        return this;
    }

    public SageClientBuilder withHTTPClient(HttpClient client) {
        setHttpClient(client);
        return this;
    }

    public SageRemoteClient build() {
        return new SageDefaultClient(url, httpClient);
    }
}
