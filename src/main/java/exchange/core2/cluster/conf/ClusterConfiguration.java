package exchange.core2.cluster.conf;

public interface ClusterConfiguration {

    int getTotalNodes();

    String getHostname(int nodeId);

    int getPort(int nodeId, AeronServiceType aeronServiceType);

    default String getNodeEndpoint(final int nodeId, final AeronServiceType aeronServiceType) {
        return getHostname(nodeId) + ":" + getPort(nodeId, aeronServiceType);
    }

}
