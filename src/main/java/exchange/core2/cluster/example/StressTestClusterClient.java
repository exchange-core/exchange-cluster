package exchange.core2.cluster.example;


import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.client.LoggingResponseHandler;
import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.model.CoreSymbolSpecification;
import exchange.core2.cluster.model.ExchangeCommandCode;
import exchange.core2.cluster.model.binary.BatchAddSymbolsCommand;
import exchange.core2.orderbook.IOrderBook;
import exchange.core2.orderbook.IResponseHandler;
import exchange.core2.orderbook.OrderAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class StressTestClusterClient {

    private static final Logger log = LoggerFactory.getLogger(StressTestClusterClient.class);

    private final ExchangeCoreClusterClient clusterClient;

    private final CompletableFuture<Boolean> l2DataIsCorrectFuture = new CompletableFuture<>();

    private static final long l2dataCorrelationId = 0xDEAL;

    public StressTestClusterClient(final String aeronDirName,
                                   final ClusterConfiguration clusterConfiguration,
                                   final String clientEndpoint) {

        final IResponseHandler responseHandler = new LoggingResponseHandler() {
            @Override
            public void onL2DataResult(short resultCode,
                                       long time,
                                       long correlationId,
                                       int symbolId,
                                       IL2Proxy l2dataProxy) {

                super.onL2DataResult(resultCode, time, correlationId, symbolId, l2dataProxy);

                log.debug("ASKS={} BIDS={}", l2dataProxy.getAskRecordsNum(), l2dataProxy.getBidRecordsNum());

                l2DataIsCorrectFuture.complete(
                        correlationId == l2dataCorrelationId
                                && l2dataProxy.getAskRecordsNum() == 0
                                && l2dataProxy.getBidRecordsNum() == 0);
            }
        };

        this.clusterClient = new ExchangeCoreClusterClient(
                aeronDirName,
                clusterConfiguration,
                clientEndpoint,
                responseHandler,
                true,
                63); // TODO specify client node index
    }


    public void performSimpleTest() {

        log.info("Sending reset...");
        clusterClient.sendNoArgsCommandAsync(0x123123123L,
                System.nanoTime(),
                ExchangeCommandCode.RESET);

        final CoreSymbolSpecification spec = new CoreSymbolSpecification(38001);
        BatchAddSymbolsCommand binaryDataCommand = new BatchAddSymbolsCommand(Collections.singletonList(spec));

        log.info("Creating symbol...");
        clusterClient.sendBinaryDataCommandAsync(
                0x2233445566778899L,
                System.nanoTime(),
                binaryDataCommand);

        log.info("Placing order1...");
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

        log.info("Placing order2...");
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

        log.info("Move order...");
        clusterClient.moveOrderAsync(
                0x22DD22DD22DD22DDL,
                System.nanoTime(),
                38001,
                1001L,
                10L,
                50_500L);

        log.info("Reduce order...");
        clusterClient.reduceOrderAsync(
                0x22DD22DD22DD22DDL,
                System.nanoTime(),
                38001,
                1001L,
                10L,
                600_000L);

        log.info("Cancel order...");
        clusterClient.cancelOrderAsync(
                0x33EE33EE33EE33EEL,
                System.nanoTime(),
                38001,
                1001L,
                10L);

        if (l2DataIsCorrectFuture.isDone()) {
            throw new IllegalStateException();
        }

        log.info("Requesting L2...");

        clusterClient.sendL2DataQueryAsync(
                l2dataCorrelationId,
                System.nanoTime(),
                38001,
                10);

        log.info("Waiting for L2...");

        l2DataIsCorrectFuture.join();
        log.info("Done");
    }

    public void shutdown() {
        clusterClient.shutdown();
        log.info("Shutdown");
    }

}
