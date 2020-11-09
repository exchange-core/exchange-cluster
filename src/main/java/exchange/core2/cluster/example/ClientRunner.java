package exchange.core2.cluster.example;

import exchange.core2.cluster.ExchangeCoreCluster;
import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.conf.ClusterConfigurationsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;

@CommandLine.Command(name = "run", description = "Run client")
public class ClientRunner implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ClientRunner.class);

    @CommandLine.Option(names = {"-m", "--mode"}, required = true)
    private ExchangeCoreCluster.ConfigurationType configurationType;

    @CommandLine.Option(names = {"-t", "--total-nodes"}, defaultValue = "3")
    private int totalNodes;

    @CommandLine.Option(names = {"-c", "--client-endpoint"}, defaultValue = "localhost:19001")
    private String clientEndpoint;

    @CommandLine.Option(names = {"-p", "--properties"})
    private String propertiesFilename;

    @CommandLine.Option(names = {"-s", "--service-mode"}, required = true)
    private ServiceMode serviceMode;

    @Override
    public void run() {

        log.info("Initializing cluster configuration...");

        final ClusterConfiguration clusterConfiguration = ClusterConfigurationsFactory.createClusterConfiguration(
                configurationType,
                totalNodes,
                propertiesFilename);

        log.info("Created: {}", clusterConfiguration);

        final String aeronDirName = new File(System.getProperty("user.dir"), "aeron-cluster-client").getAbsolutePath();

        log.info("clientEndpoint={}", clientEndpoint);

        final ExchangeCoreClusterClient clusterClient = new ExchangeCoreClusterClient(
                aeronDirName,
                clusterConfiguration,
                clientEndpoint,
                true);

        switch (serviceMode) {
            case REST_API:
                final RestApiClusterClient restApi = new RestApiClusterClient(clusterClient);
                restApi.registerRoutes();
                break;

            case TESTING:
                final StressTestClusterClient tester = new StressTestClusterClient(clusterClient);
                tester.performTest();
                break;
        }


    }

    public static void main(String[] args) {
        new CommandLine(new ClientRunner()).execute(args);
    }

    public enum ServiceMode {
        REST_API,
        TESTING
    }
}
