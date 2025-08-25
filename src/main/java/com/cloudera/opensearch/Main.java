package com.cloudera.opensearch;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContexts;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.InfoResponse;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexResponse;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.opensearch.indices.PutIndicesSettingsRequest;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import javax.net.ssl.SSLContext;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        try {
            final HttpHost host = new HttpHost("https", args[0], 9200);

            final UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("admin", "admin".toCharArray());
            final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(new AuthScope(host), credentials);

            final SSLContext sslContext = SSLContexts.custom()
                    .loadTrustMaterial(null, (x509Certificates, s) -> true)
                    .build();

            final ApacheHttpClient5TransportBuilder builder = ApacheHttpClient5TransportBuilder.builder(host)
                    .setHttpClientConfigCallback(httpClientBuilder -> {
                TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                        .setSslContext(sslContext)
                        .setTlsDetailsFactory(sslEngine -> new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol()))
                        .build();

                PoolingAsyncClientConnectionManagerBuilder connectionManagerBuilder = PoolingAsyncClientConnectionManagerBuilder
                        .create()
                        .setTlsStrategy(tlsStrategy);


                PoolingAsyncClientConnectionManager connectionManager = connectionManagerBuilder.build();
                  return httpClientBuilder
                    .setDefaultCredentialsProvider(credentialsProvider).setConnectionManager(connectionManager);
            });

            try (ApacheHttpClient5Transport transport = builder.build()) {
                OpenSearchClient openSearchClient = new OpenSearchClient(transport);
                InfoResponse info = openSearchClient.info();
                System.out.println(info.version().distribution() + ": " + info.version().number());

                //Create the index
                String index = "sample-index";
                CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder().index(index).build();
                openSearchClient.indices().create(createIndexRequest);

                //Add some settings to the index
                IndexSettings indexSettings = new IndexSettings.Builder().autoExpandReplicas("0-all").build();
                PutIndicesSettingsRequest putIndicesSettingsRequest = new PutIndicesSettingsRequest.Builder().index(index).settings(indexSettings).build();
                openSearchClient.indices().putSettings(putIndicesSettingsRequest);

                //Index some data
                IndexData indexData = new IndexData("Tamas", "Payer");
                IndexRequest.Builder<IndexData> indexRequestBuilder = new IndexRequest.Builder<>();
                IndexRequest<IndexData> indexRequest = indexRequestBuilder.index(index).id("1").document(indexData).build();
                openSearchClient.index(indexRequest);

                //Search for the document
                SearchResponse<IndexData> searchResponse = openSearchClient.search(s -> s.index(index), IndexData.class);
                for (int i = 0; i < searchResponse.hits().hits().size(); i++) {
                    System.out.println(searchResponse.hits().hits().get(i).source());
                }

                //Delete the document
                openSearchClient.delete(b -> b.index(index).id("1"));

                // Delete the index
                DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder().index(index).build();
                DeleteIndexResponse deleteIndexResponse = openSearchClient.indices().delete(deleteIndexRequest);
                System.out.println(deleteIndexResponse);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}