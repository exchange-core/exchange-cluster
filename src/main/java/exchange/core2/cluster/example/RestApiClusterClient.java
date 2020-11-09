package exchange.core2.cluster.example;


import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static spark.Spark.get;

public class RestApiClusterClient {

    private static final Logger log = LoggerFactory.getLogger(RestApiClusterClient.class);

    private final ExchangeCoreClusterClient clusterClient;

    public RestApiClusterClient(final ExchangeCoreClusterClient clusterClient) {

        this.clusterClient = clusterClient;
    }

    public void registerRoutes() {
        get("/createUser", (request, response) -> {
            log.info("Create user HTTP route");
            clusterClient.sendAddUserRequest(202);
            return "";
        });
    }

}
