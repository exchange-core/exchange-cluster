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

        clusterClient.placeOrderAsync(
                0x55FF55FF55FF55FFL,
                System.nanoTime(),
                38001,
                IOrderBook.ORDER_TYPE_GTC,
                1001L,
                10L,
                50_000L,
                51_000L,
                800_000L,
                OrderAction.BID,
                981438274);

        clusterClient.placeOrderAsync(
                123456700000002L,
                System.nanoTime(),
                38001,
                IOrderBook.ORDER_TYPE_IOC,
                1002L,
                11L,
                49_999L,
                49_999L,
                20_000L,
                OrderAction.ASK,
                -1982378279);

        clusterClient.moveOrderAsync(
                0x22DD22DD22DD22DDL,
                System.nanoTime(),
                38001,
                1001L,
                10L,
                50_500L);

        clusterClient.reduceOrderAsync(
                0x22DD22DD22DD22DDL,
                System.nanoTime(),
                38001,
                1001L,
                10L,
                600_000L);

        clusterClient.cancelOrderAsync(
                0x33EE33EE33EE33EEL,
                System.nanoTime(),
                38001,
                1001L,
                10L);

    }

}
