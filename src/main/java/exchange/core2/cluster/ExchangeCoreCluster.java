package exchange.core2.cluster;

import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.conf.ClusterConfigurationsFactory;
import exchange.core2.cluster.example.RestApiClusterClient;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "run", description = "Run cluster node")
public class ExchangeCoreCluster implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(RestApiClusterClient.class);

    @CommandLine.Option(names = {"-m", "--mode"}, required = true)
    private ConfigurationType configurationType;

    @CommandLine.Option(names = {"-t", "--total-nodes"}, defaultValue = "3")
    private int totalNodes;

    @CommandLine.Option(names = {"-n", "--node"}, required = true)
    private int nodeId;

    @CommandLine.Option(names = {"-p", "--properties"})
    private String propertiesFilename;

    @Override
    public void run() {

        log.info("Initializing cluster configuration...");
        final ClusterConfiguration clusterConfiguration = ClusterConfigurationsFactory.createClusterConfiguration(
                configurationType,
                totalNodes,
                propertiesFilename);

        log.info("Created {}", clusterConfiguration);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ExchangeCoreClusterNode clusterNode = new ExchangeCoreClusterNode(
                barrier,
                clusterConfiguration,
                Thread::new);

        clusterNode.start(nodeId, true);

        barrier.await();
    }

    public static void main(String[] args) {
        new CommandLine(new ExchangeCoreCluster()).execute(args);
    }

    public enum ConfigurationType {
        LOCAL,
        MULTISERVER
    }
}
