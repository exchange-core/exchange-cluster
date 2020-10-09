package exchange.core2.cluster;

import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.conf.ClusterLocalConfiguration;
import org.agrona.concurrent.ShutdownSignalBarrier;

public class ExchangeCoreCluster {

    public static void main(String[] args) {
        final int nodeId = Integer.parseInt(args[0]);
        final int nNodes = Integer.parseInt(args[1]);

        final ClusterConfiguration clusterConfiguration = new ClusterLocalConfiguration(nNodes);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ExchangeCoreClusterNode clusterNode = new ExchangeCoreClusterNode(barrier, clusterConfiguration);

        clusterNode.start(nodeId, nNodes, true);
        barrier.await();
    }
}
