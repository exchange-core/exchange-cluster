package exchange.core2.cluster.conf;

public class ClusterLocalConfiguration implements ClusterConfiguration {

    private static final String LOCALHOST = "localhost";
    private static final int PORT_BASE = 9000;
    private static final int PORTS_PER_NODE = 100;

    private final int totalNodes;

    public ClusterLocalConfiguration(int totalNodes) {
        this.totalNodes = totalNodes;
    }

    @Override
    public int getTotalNodes() {
        return totalNodes;
    }

    @Override
    public String getHostname(final int nodeId) {
        return LOCALHOST;
    }

    @Override
    public int getPort(final int nodeId, final AeronServiceType aeronServiceType) {
        return PORT_BASE + (nodeId * PORTS_PER_NODE) + aeronServiceType.getPortOffset();
    }
}
