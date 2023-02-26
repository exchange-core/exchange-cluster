package exchange.core2.cluster.testing;

import exchange.core2.cluster.ExchangeCoreClusterNode;
import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.conf.ClusterLocalConfiguration;
import exchange.core2.orderbook.IResponseHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class LocalTestingContainer implements TestContainer {

    private static final Logger log = LoggerFactory.getLogger(LocalTestingContainer.class);

    private final ExchangeCoreClusterClient clusterClient;
    private final List<ExchangeCoreClusterNode> clusterNodes;
    private final TestingHelperClient testingHelperClient;

    private static final String CLIENT_ENDPOINT_LOCAL_HOST = "localhost:19001";

    public static LocalTestingContainer create(final IResponseHandler responseHandler,
                                               final ClusterNodesMode clusterNodes) {

        log.info("Initializing cluster configuration {}...", clusterNodes);
        final ClusterConfiguration clusterConfiguration = new ClusterLocalConfiguration(clusterNodes.numNodes);

        log.info("Created configuration: {}", clusterConfiguration);
//        final AffinityThreadFactory threadFactory = new AffinityThreadFactory(THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE);
        final ThreadFactory threadFactory = Thread::new;

        final List<ExchangeCoreClusterNode> nodes = IntStream.range(0, clusterNodes.numNodes)
            .mapToObj(nodeIndex -> {
                final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
                log.info("Creating cluster node {} ...", nodeIndex);
                final ExchangeCoreClusterNode clusterNode = new ExchangeCoreClusterNode(
                    barrier,
                    clusterConfiguration,
                    threadFactory);

                log.info("Starting cluster node {} ...", nodeIndex);
                clusterNode.start(nodeIndex, true);
                return clusterNode;
            })
            .collect(Collectors.toList());

        log.info("Creating client ...");

        final String aeronDirName = new File(System.getProperty("client.dir"), "aeron-cluster-client").getAbsolutePath();

        final ExchangeCoreClusterClient clusterClient = new ExchangeCoreClusterClient(
            aeronDirName,
            clusterConfiguration,
            CLIENT_ENDPOINT_LOCAL_HOST,
            responseHandler,
            true,
            1);

        final TestingHelperClient testingHelperClient = new TestingHelperClient(clusterClient);

        return new LocalTestingContainer(clusterClient, nodes, testingHelperClient);
    }

    private LocalTestingContainer(ExchangeCoreClusterClient clusterClient,
                                  List<ExchangeCoreClusterNode> clusterNodes,
                                  TestingHelperClient testingHelperClient) {

        this.clusterClient = clusterClient;
        this.clusterNodes = clusterNodes;
        this.testingHelperClient = testingHelperClient;
    }

    public ExchangeCoreClusterClient getClusterClient() {
        return clusterClient;
    }

    public Stream<ExchangeCoreClusterNode> getClusterNodes() {
        return clusterNodes.stream();
    }

    public TestingHelperClient getTestingHelperClient() {
        return testingHelperClient;
    }


    @Override
    public void close() {

        clusterClient.shutdown();
        clusterNodes.forEach(ExchangeCoreClusterNode::stop);

        //log.debug("await...");
        //barrier.await();
        //log.debug("after await");
    }

    public enum ClusterNodesMode {
        SINGLE_NODE(1),
        MULTI(3);

        ClusterNodesMode(int numNodes) {
            this.numNodes = numNodes;
        }

        final int numNodes;
    }

}
