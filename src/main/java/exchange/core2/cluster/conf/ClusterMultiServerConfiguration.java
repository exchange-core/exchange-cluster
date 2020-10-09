package exchange.core2.cluster.conf;

public class ClusterMultiServerConfiguration implements ClusterConfiguration {

    private final int basePort;
    private final String[] hostnames;

    // TODO read from properties

    public ClusterMultiServerConfiguration(int basePort, String[] hostnames) {
        this.basePort = basePort;
        this.hostnames = hostnames;
    }

    @Override
    public int getTotalNodes() {
        return hostnames.length;
    }

    @Override
    public String getHostname(int nodeId) {
        return hostnames[nodeId];
    }

    @Override
    public int getPort(int nodeId, AeronServiceType aeronServiceType) {
        return basePort + aeronServiceType.getPortOffset();
    }
}