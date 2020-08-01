package exchange.core2.cluster;

import org.agrona.concurrent.ShutdownSignalBarrier;

public class ExchangeCoreCluster {

    public static void main(String[] args) {
        final int nodeId = Integer.parseInt(args[0]);
        final int nNodes = Integer.parseInt(args[1]);
        ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        ExchangeCoreClusterNode clusterNode = new ExchangeCoreClusterNode(barrier);
        clusterNode.start(nodeId, nNodes,true);
        barrier.await();
    }
}
