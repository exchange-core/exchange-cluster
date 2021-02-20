package exchange.core2.cluster.testing;

import exchange.core2.cluster.ExchangeCoreClusterNode;
import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.conf.ClusterLocalConfiguration;
import exchange.core2.cluster.utils.AffinityThreadFactory;
import exchange.core2.orderbook.IResponseHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public final class SingleNodeTestingContainer implements TestContainer {

    private static final Logger log = LoggerFactory.getLogger(SingleNodeTestingContainer.class);

    private final ExchangeCoreClusterClient clusterClient;
    private final ExchangeCoreClusterNode clusterNode;
    private final TestingHelperClient testingHelperClient;

    private static final String CLIENT_ENDPOINT_LOCAL_HOST = "localhost:19001";

    public static SingleNodeTestingContainer create(final IResponseHandler responseHandler) {

        log.info("Initializing cluster configuration...");
        final ClusterConfiguration clusterConfiguration = new ClusterLocalConfiguration(1);

        log.info("Created {}", clusterConfiguration);

        final AffinityThreadFactory threadFactory = new AffinityThreadFactory(
                AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        final ExchangeCoreClusterNode clusterNode = new ExchangeCoreClusterNode(
                barrier,
                clusterConfiguration,
                threadFactory);

        clusterNode.start(0, true);

        final String aeronDirName = new File(System.getProperty("client.dir"), "aeron-cluster-client").getAbsolutePath();

        final ExchangeCoreClusterClient clusterClient = new ExchangeCoreClusterClient(
                aeronDirName,
                clusterConfiguration,
                CLIENT_ENDPOINT_LOCAL_HOST,
                responseHandler,
                true,
                1);

        final TestingHelperClient testingHelperClient = new TestingHelperClient(clusterClient);

        return new SingleNodeTestingContainer(clusterClient, clusterNode, testingHelperClient);
    }

    private SingleNodeTestingContainer(ExchangeCoreClusterClient clusterClient,
                                       ExchangeCoreClusterNode clusterNode,
                                       TestingHelperClient testingHelperClient) {

        this.clusterClient = clusterClient;
        this.clusterNode = clusterNode;
        this.testingHelperClient = testingHelperClient;
    }

    public ExchangeCoreClusterClient getClusterClient() {
        return clusterClient;
    }

    public ExchangeCoreClusterNode getClusterNode() {
        return clusterNode;
    }

    public TestingHelperClient getTestingHelperClient() {
        return testingHelperClient;
    }


    @Override
    public void close() {

        clusterClient.shutdown();

        clusterNode.stop();

        //log.debug("await...");
        //barrier.await();
        //log.debug("after await");
    }
}
