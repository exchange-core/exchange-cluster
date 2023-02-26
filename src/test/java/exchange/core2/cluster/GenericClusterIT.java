package exchange.core2.cluster;

import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.client.LoggingResponseHandler;
import exchange.core2.cluster.model.CoreSymbolSpecification;
import exchange.core2.cluster.model.binary.BatchAddSymbolsCommand;
import exchange.core2.cluster.testing.LocalTestingContainer;
import exchange.core2.orderbook.IOrderBook;
import exchange.core2.orderbook.IResponseHandler;
import exchange.core2.orderbook.OrderAction;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static exchange.core2.cluster.testing.LocalTestingContainer.ClusterNodesMode.SINGLE_NODE;
import static org.junit.Assert.assertTrue;


public class GenericClusterIT {


    private static final Logger log = LoggerFactory.getLogger(GenericClusterIT.class);

    @Test
    public void basicTest() {

        final CompletableFuture<Boolean> l2DataIsCorrectFuture = new CompletableFuture<>();

        final IResponseHandler responseHandler = new LoggingResponseHandler() {
            @Override
            public void onL2DataResult(short resultCode, long time, long correlationId, int symbolId, IL2Proxy l2dataProxy) {
                super.onL2DataResult(resultCode, time, correlationId, symbolId, l2dataProxy);
                log.debug("ASKS={} BIDS={}", l2dataProxy.getAskRecordsNum(), l2dataProxy.getBidRecordsNum());
                l2DataIsCorrectFuture.complete(correlationId == 0xDEAL && l2dataProxy.getAskRecordsNum() == 0 && l2dataProxy.getBidRecordsNum() == 0);
            }
        };

        try (LocalTestingContainer cont = LocalTestingContainer.create(responseHandler, SINGLE_NODE)) {

            final ExchangeCoreClusterClient clusterClient = cont.getClusterClient();

            final CoreSymbolSpecification spec = new CoreSymbolSpecification(38001);
            BatchAddSymbolsCommand binaryDataCommand = new BatchAddSymbolsCommand(Collections.singletonList(spec));
            clusterClient.sendBinaryDataCommandAsync(
                    0x2233_4455_6677_8899L,
                    System.nanoTime(),
                    binaryDataCommand);

            clusterClient.placeOrderAsync(
                    0x55FF_55FF_55FF_55FFL,
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
                    0x22DD_22DD_22DD_22DDL,
                    System.nanoTime(),
                    38001,
                    1001L,
                    10L,
                    50_500L);

            clusterClient.reduceOrderAsync(
                    0x33DD_33DD_33DD_33DDL,
                    System.nanoTime(),
                    38001,
                    1001L,
                    10L,
                    600_000L);

            clusterClient.cancelOrderAsync(
                    0x33EE_33EE_33EE_33EEL,
                    System.nanoTime(),
                    38001,
                    1001L,
                    10L);

            clusterClient.sendL2DataQueryAsync(
                    0xDEAL,
                    System.nanoTime(),
                    38001,
                    10);

            assertTrue(l2DataIsCorrectFuture.join());

        }

    }


}
