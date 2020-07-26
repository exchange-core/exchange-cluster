package exchange.core2.cluster.example;


import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static spark.Spark.get;

public class SampleExchangeCoreClusterClient extends ExchangeCoreClusterClient {

    private static final Logger log = LoggerFactory.getLogger(SampleExchangeCoreClusterClient.class);

    public SampleExchangeCoreClusterClient(String aeronDirName, String ingressHost, String egressHost, int egressPort) {
        super(aeronDirName, ingressHost, egressHost, egressPort);
    }

    private void registerRoutes() {
        get("/createUser", (request, response) -> {
            log.info("Create user HTTP route");
            return sendAddUserRequest(202).toString();
        });
    }

    public static void main(String[] args) {
        final String aeronDirName = new File(System.getProperty("user.dir"), "aeron-cluster-client").getAbsolutePath();
        final String LOCALHOST = "localhost";
        final int egressPort = 19001;  // change for different clients

        SampleExchangeCoreClusterClient clusterClient = new SampleExchangeCoreClusterClient(
                aeronDirName, LOCALHOST, LOCALHOST, egressPort
        );

        clusterClient.connectToCluster();
        clusterClient.registerRoutes();
    }
}
