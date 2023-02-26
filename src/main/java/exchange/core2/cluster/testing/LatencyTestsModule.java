package exchange.core2.cluster.testing;

import exchange.core2.benchmarks.generator.orders.MultiSymbolGenResult;
import exchange.core2.benchmarks.generator.util.LatencyTools;
import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.client.IgnoringResponseHandler;
import exchange.core2.orderbook.IResponseHandler;
import exchange.core2.orderbook.OrderAction;
import exchange.core2.orderbook.util.BufferReader;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

public class LatencyTestsModule {

    private static final Logger log = LoggerFactory.getLogger(LatencyTestsModule.class);

    private static final boolean WRITE_HDR_HISTOGRAMS = false;

    private static final int MAX_MEDIAN_LATENCY_NS = 100 * 1_000_000; //100ms

    public static void latencyTestImpl(final TestDataParameters testDataParameters,
                                       final Function<IResponseHandler, TestContainer> testContainerFactory,
                                       final int warmupCycles) {

        final int initialTps = 5_000; // initial transactions per second
        final int incrementTpsBy = 5_000;

        final int warmupTps = 10_000;

        final TestDataFutures testDataFutures = TestDataGenerationHelper.initiateTestDataGeneration(testDataParameters, 1);

        final SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

        final AtomicLong lastCorrelationId = new AtomicLong();

        final IResponseHandler responseHandler = new IgnoringResponseHandler() {
            @Override
            public void onOrderPlaceResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted, int userCookie, long remainingSize) {
                handle(correlationId);
            }

            @Override
            public void onOrderCancelResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted) {
                handle(correlationId);
            }

            @Override
            public void onOrderMoveResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted, long remainingSize) {
                handle(correlationId);
            }

            @Override
            public void onOrderReduceResult(short resultCode, long time, long correlationId, int symbolId, long uid, long orderId, OrderAction action, boolean orderCompleted, long remainingSize) {
                handle(correlationId);
            }

            private void handle(final long correlationId) {
                final long latency = System.nanoTime() - correlationId;
                hdrRecorder.recordValue(Math.min(latency, Integer.MAX_VALUE));
                lastCorrelationId.lazySet(correlationId);
            }

        };


        try (TestContainer container = testContainerFactory.apply(responseHandler)) {


            final TestingHelperClient testingHelperClient = container.getTestingHelperClient();

            // TODO - first run should validate the output (orders are accepted and processed properly)

            final BiFunction<Integer, Boolean, Boolean> testIteration = (tps, warmup) -> {
                try {
                    testingHelperClient.loadSymbolsClientsAndPreFillOrders(testDataFutures);

                    final MultiSymbolGenResult genResult = testDataFutures.getGenResult().join();
                    final BufferReader commandsBuffer = genResult.getCommandsBenchmark().join();
                    final ExchangeCoreClusterClient clusterClient = testingHelperClient.getClusterClient();

                    Thread.sleep(200);

                    final long t = System.nanoTime();
                    clusterClient.cancelOrderAsync(t, t, -1, -1, -1);
                    //clusterClient.sendNoArgsCommandAsync(t, t, ExchangeCommandCode.NOP);
                    while (lastCorrelationId.get() != t) {
                        Thread.yield();
                    }
                    hdrRecorder.reset();

                    final int nanosPerCmd = 1_000_000_000 / tps;
                    final long startTimeMs = System.currentTimeMillis();

                    commandsBuffer.reset();

                    long plannedTimestamp = System.nanoTime();

                    while (commandsBuffer.getRemainingSize() > 0) {

                        plannedTimestamp += nanosPerCmd;

                        final byte cmd = commandsBuffer.readByte();
                        final int symbolId = commandsBuffer.readInt();

                        // log.debug("Sending command  cmd={} symbolId={} correlationId={}", cmd, symbolId, correlationId);

                        while (System.nanoTime() < plannedTimestamp) {
                            // spin until its time to send next command
                        }

                        clusterClient.placePreparedCommandMultiSymAsync(
                                plannedTimestamp,
                                plannedTimestamp,
                                symbolId,
                                cmd,
                                commandsBuffer);
                    }


                    while (lastCorrelationId.get() != plannedTimestamp) {
                        Thread.yield();
                    }


                    final long processingTimeMs = System.currentTimeMillis() - startTimeMs;
                    final float perfMt = (float) genResult.getBenchmarkCommandsSize() / (float) processingTimeMs / 1000.0f;
                    String tag = String.format("%.3f MT/s", perfMt);
                    final Histogram histogram = hdrRecorder.getIntervalHistogram();
                    log.info("{} {}", tag, LatencyTools.createLatencyReportFast(histogram));

                    // compare orderBook final state just to make sure all commands executed same way
//                    testDataFutures.coreSymbolSpecifications.join().forEach(
//                            symbol -> assertEquals(
//                                    genResult.getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(),
//                                    container.requestCurrentOrderBook(symbol.symbolId)));

                    // TODO compare events, balances, positions

                    if (WRITE_HDR_HISTOGRAMS) {
                        final PrintStream printStream = new PrintStream(new File(System.currentTimeMillis() + "-" + perfMt + ".perc"));
                        //log.info("HDR 50%:{}", hdr.getValueAtPercentile(50));
                        histogram.outputPercentileDistribution(printStream, 1000.0);
                    }

                    testingHelperClient.sendResetAsync();

                    //System.gc();
                    Thread.sleep(200);

                    // stop testing if median latency above 1 millisecond
                    return warmup || histogram.getValueAtPercentile(50.0) < MAX_MEDIAN_LATENCY_NS;

                } catch (InterruptedException | FileNotFoundException ex) {
                    throw new IllegalStateException(ex);
                }
            };

            testingHelperClient.executeTestingThread(() -> {
                log.debug("Warming up {} cycles...", warmupCycles);
                IntStream.range(0, warmupCycles)
                        .forEach(i -> testIteration.apply(warmupTps, true));
                log.debug("Warmup done, starting tests");

                return IntStream.range(0, 10000)
                        .map(i -> initialTps + incrementTpsBy * i)
                        .mapToObj(tps -> testIteration.apply(tps, false))
                        .allMatch(x -> x);
            });
        }
    }
}
