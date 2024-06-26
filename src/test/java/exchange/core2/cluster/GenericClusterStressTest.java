package exchange.core2.cluster;

import exchange.core2.benchmarks.generator.Constants;
import exchange.core2.benchmarks.generator.GeneratorSymbolSpec;
import exchange.core2.benchmarks.generator.orders.GenResult;
import exchange.core2.benchmarks.generator.orders.SingleBookOrderGenerator;
import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.client.IgnoringResponseHandler;
import exchange.core2.cluster.model.CoreSymbolSpecification;
import exchange.core2.cluster.model.binary.BatchAddSymbolsCommand;
import exchange.core2.cluster.testing.LocalTestingContainer;
import exchange.core2.cluster.testing.TestingHelperClient;
import exchange.core2.orderbook.IResponseHandler;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static exchange.core2.cluster.testing.LocalTestingContainer.ClusterNodesMode.SINGLE_NODE;
import static org.junit.Assert.assertTrue;

public class GenericClusterStressTest {


    private static final Logger log = LoggerFactory.getLogger(GenericClusterIT.class);

    @Test
    public void runStressTest() {

        final GeneratorSymbolSpec symbolSpec = Constants.SYMBOLSPECFEE_XBT_LTC;

        final int symbolId = symbolSpec.getSymbolId();


        final GenResult genResult = SingleBookOrderGenerator.generateCommands(
                50_000,
                1000,
                1000,
                i -> i + 1,
                symbolSpec,
                false,
                false,
                a -> {
                },
                1,
                1);

        log.debug("benchmark size: {} bytes", genResult.getCommandsBenchmark().getSize());

        final CompletableFuture<Boolean> l2DataIsCorrectFuture = new CompletableFuture<>();

        final IResponseHandler responseHandler = new IgnoringResponseHandler() {
            @Override
            public void onL2DataResult(short resultCode, long time, long correlationId, int symbolId, IL2Proxy l2dataProxy) {
                super.onL2DataResult(resultCode, time, correlationId, symbolId, l2dataProxy);
                log.debug("ASKS={} BIDS={}", l2dataProxy.getAskRecordsNum(), l2dataProxy.getBidRecordsNum());
                if (correlationId == 0xDEAL) {
                    l2DataIsCorrectFuture.complete(true);
                }
            }
        };

        long c = 0;

        try (final LocalTestingContainer cont = LocalTestingContainer.create(responseHandler, SINGLE_NODE)) {

            final ExchangeCoreClusterClient clusterClient = cont.getClusterClient();
            final TestingHelperClient testingHelperClient = cont.getTestingHelperClient();
            final CoreSymbolSpecification spec = new CoreSymbolSpecification(symbolId);

            BatchAddSymbolsCommand binaryDataCommand = new BatchAddSymbolsCommand(Collections.singletonList(spec));
            clusterClient.sendBinaryDataCommandAsync(
                    0x2233_4455_6677_8899L,
                    System.nanoTime(),
                    binaryDataCommand);

            testingHelperClient.sendCommandsAsync(c, symbolId, genResult.getCommandsFill());
            final long startTimeMs = System.currentTimeMillis();
            testingHelperClient.sendCommandsAsync(c, symbolId, genResult.getCommandsBenchmark());

            clusterClient.sendL2DataQueryAsync(
                    0xDEAL,
                    System.nanoTime(),
                    symbolId,
                    10);

            assertTrue(l2DataIsCorrectFuture.join());

            final long timeMs = System.currentTimeMillis() - startTimeMs;
            float mps = (float) genResult.getNumCommandsBenchmark() / ((float) timeMs * 1000.0f);
            log.info("MPS: {}", String.format("%.3f", mps));
        }
    }


}
