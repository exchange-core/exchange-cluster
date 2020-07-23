package exchange.core2.cluster.example;


import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import org.agrona.DirectBuffer;

import java.io.File;

public class SampleExchangeCoreClusterClient extends ExchangeCoreClusterClient {

    public SampleExchangeCoreClusterClient(String aeronDirName, String ingressHost, String egressHost, int egressPort) {
        super(aeronDirName, ingressHost, egressHost, egressPort);
    }

    public static void main(String[] args) {
        final String aeronDirName = new File(System.getProperty("user.dir"), "aeron-cluster-client").getAbsolutePath();
        final String LOCALHOST = "localhost";
        final int egressPort = 19001;  // change for different clients

        ExchangeCoreClusterClient clusterClient = new SampleExchangeCoreClusterClient(
                aeronDirName, LOCALHOST, LOCALHOST, egressPort
        );

        clusterClient.connectToCluster();

        new Thread(() -> {
            while (true) clusterClient.pollEgress();
        }).start();

//        DirectBuffer buffer = clusterClient.sendCreateUserRequest(101);
//        System.out.println("HTTP client received buffer: " + buffer);
    }
}
