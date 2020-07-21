package exchange.core2.cluster;

import org.agrona.concurrent.ShutdownSignalBarrier;

public class ExchangeCoreCluster {
    public static void main(String[] args) {
        ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        ExchangeCoreClusterNode clusterNode = new ExchangeCoreClusterNode(barrier);
        clusterNode.start(false);
        barrier.await();
    }
}
