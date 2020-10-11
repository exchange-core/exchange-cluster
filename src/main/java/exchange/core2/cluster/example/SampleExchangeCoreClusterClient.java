package exchange.core2.cluster.example;


import exchange.core2.cluster.ExchangeCoreCluster;
import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.conf.ClusterConfigurationsFactory;
import exchange.core2.cluster.conf.ClusterLocalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;

import static spark.Spark.get;

public class SampleExchangeCoreClusterClient extends ExchangeCoreClusterClient {

    private static final Logger log = LoggerFactory.getLogger(SampleExchangeCoreClusterClient.class);



    public SampleExchangeCoreClusterClient(final String aeronDirName,
                                           final ClusterConfiguration clusterConfiguration,
                                            final String egressChannelEndpoint) {

        super(aeronDirName, clusterConfiguration, egressChannelEndpoint, true);
    }

    public void registerRoutes() {
        get("/createUser", (request, response) -> {
            log.info("Create user HTTP route");
            return sendAddUserRequest(202).toString();
        });
    }

}
