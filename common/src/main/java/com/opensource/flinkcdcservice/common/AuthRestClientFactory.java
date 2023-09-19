package com.opensource.flinkcdcservice.common;

import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClientBuilder;

import javax.annotation.Nullable;
import java.util.Objects;

public class AuthRestClientFactory implements RestClientFactory {
    private final String pathPrefix;
    private final String username;
    private final String password;
    private transient CredentialsProvider credentialsProvider;

    public AuthRestClientFactory(@Nullable String pathPrefix, String username, String password) {
        this.pathPrefix = pathPrefix;
        this.password = password;
        this.username = username;
    }

    public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
        if (this.pathPrefix != null) {
            restClientBuilder.setPathPrefix(this.pathPrefix);
        }

        if (this.credentialsProvider == null) {
            this.credentialsProvider = new BasicCredentialsProvider();
            this.credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(this.username, this.password));
        }

        restClientBuilder.setHttpClientConfigCallback((httpAsyncClientBuilder) -> {
            return httpAsyncClientBuilder.setDefaultCredentialsProvider(this.credentialsProvider);
        });
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            AuthRestClientFactory that = (AuthRestClientFactory)o;
            return Objects.equals(this.pathPrefix, that.pathPrefix) && Objects.equals(this.username, that.username) && Objects.equals(this.password, that.password);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(this.pathPrefix, this.password, this.username);
    }
}
