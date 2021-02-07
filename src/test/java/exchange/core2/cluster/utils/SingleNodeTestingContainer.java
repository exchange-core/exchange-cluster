package exchange.core2.cluster.utils;

import exchange.core2.cluster.ExchangeCoreClusterNode;
import exchange.core2.cluster.GenericClusterIT;
import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.conf.ClusterLocalConfiguration;
import exchange.core2.orderbook.IResponseHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public final class SingleNodeTestingContainer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GenericClusterIT.class);

    private final ExchangeCoreClusterClient clusterClient;
    private final ExchangeCoreClusterNode clusterNode;

    private static final String CLIENT_ENDPOINT_LOCAL_HOST = "localhost:19001";

    public static SingleNodeTestingContainer create(final IResponseHandler responseHandler) {

        log.info("Initializing cluster configuration...");
        final ClusterConfiguration clusterConfiguration = new ClusterLocalConfiguration(1);

        log.info("Created {}", clusterConfiguration);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ExchangeCoreClusterNode clusterNode = new ExchangeCoreClusterNode(barrier, clusterConfiguration);

        clusterNode.start(0, true);

        final String aeronDirName = new File(System.getProperty("user.dir"), "aeron-cluster-client").getAbsolutePath();

        final ExchangeCoreClusterClient clusterClient = new ExchangeCoreClusterClient(
                aeronDirName,
                clusterConfiguration,
                CLIENT_ENDPOINT_LOCAL_HOST,
                responseHandler,
                true);


        return new SingleNodeTestingContainer(clusterClient, clusterNode);
    }

    private SingleNodeTestingContainer(ExchangeCoreClusterClient clusterClient, ExchangeCoreClusterNode clusterNode) {
        this.clusterClient = clusterClient;
        this.clusterNode = clusterNode;
    }

    public ExchangeCoreClusterClient getClusterClient() {
        return clusterClient;
    }

    public ExchangeCoreClusterNode getClusterNode() {
        return clusterNode;
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
