package exchange.core2.cluster.example;

import exchange.core2.cluster.ExchangeCoreCluster;
import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.conf.ClusterConfigurationsFactory;
import exchange.core2.cluster.testing.RemoteClusterTestingContainer;
import exchange.core2.cluster.testing.TestDataParameters;
import exchange.core2.cluster.testing.ThroughputTestsModule;
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


        switch (serviceMode) {
//            case REST_API:
//                final RestApiClusterClient restApi = new RestApiClusterClient(clusterClient);
//                restApi.registerRoutes();
//                break;

            case TESTING:
                final StressTestClusterClient tester = new StressTestClusterClient(aeronDirName, clusterConfiguration, clientEndpoint);
                tester.performSimpleTest();
                tester.shutdown();
                break;

            case STRESS_SMALL:
                ThroughputTestsModule.throughputTestImpl(
                        TestDataParameters.small(),
                        responseHandler -> RemoteClusterTestingContainer.create(
                                responseHandler,
                                clientEndpoint,
                                aeronDirName,
                                clusterConfiguration),
                        10);
                break;

            default:
                throw new UnsupportedOperationException("serviceMode " + serviceMode + " is not supported");
        }


    }

    public static void main(String[] args) {
        new CommandLine(new ClientRunner()).execute(args);
    }

    public enum ServiceMode {
        REST_API,
        TESTING,
        STRESS_SMALL,
        STRESS_MEDIUM,
        STRESS_LARGE
    }
}
