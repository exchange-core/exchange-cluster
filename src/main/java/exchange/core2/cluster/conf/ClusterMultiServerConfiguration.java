package exchange.core2.cluster.conf;

import java.util.Arrays;

public class ClusterMultiServerConfiguration implements ClusterConfiguration {

    private final int basePort;
    private final String[] hostnames;

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

    @Override
    public String toString() {
        return "ClusterMultiServerConfiguration{" +
                "basePort=" + basePort +
                ", hostnames=" + Arrays.toString(hostnames) +
                '}';
    }
}