package org.gdd.sage.http;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * Builder used to create instances of SageRemoteClient
 * @author Thomas Minier
 */
public class SageClientBuilder {
    private String url;
    private HttpClient httpClient;

    private SageClientBuilder() {}

    /**
     * Create a default SageRemoteClient
     * @param url - URL of the SaGe server to use
     * @return A new SageRemoteClient with default configuration
     */
    public static SageRemoteClient createDefault(String url) {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        HttpClient client = HttpClientBuilder.create()
                .setConnectionManager(connectionManager)
                .setUserAgent("Sage-Jena client/1.0")
                .build();
        return SageClientBuilder.create()
                .withURL(url)
                .withHTTPClient(client)
                .build();
    }

    /**
     * Get a builder used to create a SageRemoteClient
     * @return A new SageClientBuilder, ready to be configured
     */
    public static SageClientBuilder create() {
        return new SageClientBuilder();
    }

    /**
     * Set the URL of the Sage server which will be used by the client
     * @param url - The URL of the Sage server
     * @return The SageClientBuilder instance, used for chaining calls
     */
    public SageClientBuilder withURL(String url) {
        this.url = url;
        return this;
    }

    /**
     * Set the HTTP client used by the SageRemoteClient
     * @param client - The HTTP client to use
     * @return The SageClientBuilder instance, used for chaining calls
     */
    public SageClientBuilder withHTTPClient(HttpClient client) {
        this.httpClient = client;
        return this;
    }

    /**
     * Build a new SageRemoteClient
     * @return The SageRemoteClient instance
     */
    public SageRemoteClient build() {
        return new SageDefaultClient(url, httpClient);
    }
}
