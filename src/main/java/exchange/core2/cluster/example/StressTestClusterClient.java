package exchange.core2.cluster.example;


import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.model.CoreSymbolSpecification;
import exchange.core2.cluster.model.binary.BatchAddSymbolsCommand;
import exchange.core2.orderbook.IOrderBook;
import exchange.core2.orderbook.OrderAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class StressTestClusterClient {

    private static final Logger log = LoggerFactory.getLogger(StressTestClusterClient.class);

    private final ExchangeCoreClusterClient clusterClient;

    public StressTestClusterClient(final ExchangeCoreClusterClient clusterClient) {

        this.clusterClient = clusterClient;
    }


    public void performTest() {
        final CoreSymbolSpecification spec = new CoreSymbolSpecification(38001);
        BatchAddSymbolsCommand binaryDataCommand = new BatchAddSymbolsCommand(Collections.singletonList(spec));
        clusterClient.sendBinaryDataCommand(
                0x2233445566778899L,
                System.nanoTime(),
                binaryDataCommand);

        clusterClient.placeOrder(
                0x55FF55FF55FF55FFL,
                System.nanoTime(),
                38001,
                IOrderBook.ORDER_TYPE_GTC,
                1001L,
                10L,
                50_000L,
                50_000L,
                800_000L,
                OrderAction.BID,
                981438274);

        clusterClient.placeOrder(
                123456700000002L,
                System.nanoTime(),
                38001,
                IOrderBook.ORDER_TYPE_IOC,
                1002L,
                11L,
                49_999L,
                49_999L,
                100_000L,
                OrderAction.ASK,
                -1982378279);

    }

}
