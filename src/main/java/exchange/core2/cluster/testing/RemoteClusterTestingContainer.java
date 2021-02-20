package exchange.core2.cluster.testing;

import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.orderbook.IResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RemoteClusterTestingContainer implements TestContainer {

    private static final Logger log = LoggerFactory.getLogger(RemoteClusterTestingContainer.class);

    private final ExchangeCoreClusterClient clusterClient;
    private final TestingHelperClient testingHelperClient;

    public static TestContainer create(final IResponseHandler responseHandler,
                                       final String clientEndpoint,
                                       final String aeronDirName,
                                       final ClusterConfiguration clusterConfiguration) {

        final ExchangeCoreClusterClient clusterClient = new ExchangeCoreClusterClient(
                aeronDirName,
                clusterConfiguration,
                clientEndpoint,
                responseHandler,
                true,
                1);

        final TestingHelperClient testingHelperClient = new TestingHelperClient(clusterClient);

        return new RemoteClusterTestingContainer(clusterClient, testingHelperClient);
    }

    private RemoteClusterTestingContainer(ExchangeCoreClusterClient clusterClient,
                                          TestingHelperClient testingHelperClient) {

        this.clusterClient = clusterClient;
        this.testingHelperClient = testingHelperClient;
    }

    public ExchangeCoreClusterClient getClusterClient() {
        return clusterClient;
    }

    public TestingHelperClient getTestingHelperClient() {
        return testingHelperClient;
    }

    @Override
    public void close() {
        clusterClient.shutdown();
    }
}
