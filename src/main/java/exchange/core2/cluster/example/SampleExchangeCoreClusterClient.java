package exchange.core2.cluster.example;


import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.conf.ClusterLocalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static spark.Spark.get;

public class SampleExchangeCoreClusterClient extends ExchangeCoreClusterClient {

    private static final Logger log = LoggerFactory.getLogger(SampleExchangeCoreClusterClient.class);

    public SampleExchangeCoreClusterClient(final String aeronDirName,
                                           final ClusterConfiguration clusterConfiguration,
                                           final String egressChannelEndpoint) {

        super(aeronDirName, clusterConfiguration, egressChannelEndpoint, true);
    }

    private void registerRoutes() {
        get("/createUser", (request, response) -> {
            log.info("Create user HTTP route");
            return sendAddUserRequest(202).toString();
        });
    }

    public static void main(String[] args) {

        final String aeronDirName = new File(System.getProperty("user.dir"), "aeron-cluster-client").getAbsolutePath();

        final String clientEndpoint = "localhost:19001"; // change port for different clients

        final ClusterConfiguration clusterConfiguration = new ClusterLocalConfiguration(3);

        final SampleExchangeCoreClusterClient clusterClient = new SampleExchangeCoreClusterClient(
                aeronDirName,
                clusterConfiguration,
                clientEndpoint);

        clusterClient.connectToCluster();
        clusterClient.registerRoutes();
    }
}
