package exchange.core2.cluster.testing;

import com.google.common.collect.Lists;
import exchange.core2.benchmarks.generator.GeneratorSymbolSpec;
import exchange.core2.benchmarks.generator.orders.MultiSymbolGenResult;
import exchange.core2.benchmarks.generator.util.ExecutionTime;
import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.model.CoreSymbolSpecification;
import exchange.core2.cluster.model.ExchangeCommandCode;
import exchange.core2.cluster.model.binary.BatchAddSymbolsCommand;
import exchange.core2.cluster.model.binary.BinaryDataCommand;
import exchange.core2.cluster.model.binary.BinaryDataResult;
import exchange.core2.cluster.utils.AffinityThreadFactory;
import exchange.core2.orderbook.IOrderBook;
import exchange.core2.orderbook.util.BufferReader;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class TestingHelperClient {

    private static final Logger log = LoggerFactory.getLogger(TestingHelperClient.class);

    private final ExchangeCoreClusterClient clusterClient;
    private final AffinityThreadFactory threadFactory;

    public TestingHelperClient(ExchangeCoreClusterClient clusterClient) {

        // TODO fix
        this.threadFactory = new AffinityThreadFactory(AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE);
        this.clusterClient = clusterClient;
    }


    public ExchangeCoreClusterClient getClusterClient() {
        return clusterClient;
    }

    public void loadSymbolsClientsAndPreFillOrders(final TestDataFutures testDataFutures) {
        loadSymbolsClientsAndPreFillOrders(testDataFutures, false);
    }

    public void loadSymbolsClientsAndPreFillOrders(final TestDataFutures testDataFutures, boolean enableLogging) {

        // load symbols

        final List<Pair<GeneratorSymbolSpec, Double>> symbolSpecs = testDataFutures.getSymbolSpecs().join();

        if (enableLogging) {
            log.info("Loading {} symbols...", symbolSpecs.size());
            try (ExecutionTime ignore = new ExecutionTime(t -> log.debug("Loaded all symbols in {}", t))) {
                addSymbols(symbolSpecs);
            }
        } else {
            addSymbols(symbolSpecs);
        }

        // create accounts and deposit initial funds
//        final List<BitSet> clientAccounts = testDataFutures.getClientAccounts().join();
//        log.info("Loading {} clients having {} accounts...", clientAccounts.size(), clientAccounts.stream().mapToInt(BitSet::cardinality).sum());
//        try (ExecutionTime ignore = new ExecutionTime(t -> log.debug("Loaded all clients in {}", t))) {
//            clientAccountsInit(clientAccounts);
//        }

        final MultiSymbolGenResult genResult = testDataFutures.getGenResult().join();

        final BufferReader commandsFill = genResult.getCommandsFill().join();

        if (enableLogging) {
            log.info("Order books pre-fill with orders ({} KB)...", commandsFill.getSize() / 1024);
            try (ExecutionTime ignore = new ExecutionTime(t -> log.debug("Order books pre-fill completed in {}", t))) {
                sendMultiSymbolGenCommandsAsync(0, commandsFill);
            }
        } else {
            sendMultiSymbolGenCommandsAsync(0, commandsFill);
        }

        // TODO assertTrue(totalBalanceReport().isGlobalBalancesAllZero());
    }

//    public void loadSymbolsclientsAndPrefillOrdersNoLog(TestDataFutures testDataFutures) {
//
//        // load symbols
//        addSymbols(testDataFutures.coreSymbolSpecifications.join());
//
//        // create accounts and deposit initial funds
//        clientAccountsInit(testDataFutures.clientsAccounts.join());
//
//        getApi().submitCommandsSync(testDataFutures.genResult.join().getApiCommandsFill().join());
//    }


    public <R extends BinaryDataResult> void sendBinaryDataCommandSync(final BinaryDataCommand<R> data,
                                                                       final int timeOutMs) {

        final Future<R> future = clusterClient.sendCommandSync(data);

        try {
            final short resultCode = future.get(timeOutMs, TimeUnit.MILLISECONDS).getResultCode();
            if (resultCode != IOrderBook.RESULT_SUCCESS) {
                throw new RuntimeException("Unexpected resultCode=" + resultCode);
            }
        } catch (final InterruptedException | ExecutionException | TimeoutException ex) {
            log.error("Failed sending binary data command", ex);
            throw new RuntimeException(ex);
        }
    }

    public void addSymbols(final List<Pair<GeneratorSymbolSpec, Double>> symbolSpecs) {
        // split by chunks
        Lists.partition(symbolSpecs, 64).forEach(
                partition -> {
                    final List<CoreSymbolSpecification> batch = partition.stream().map(s -> toCoreSymbolSpecification(s.getFirst())).collect(Collectors.toList());
                    final BatchAddSymbolsCommand batchAddSymbolsCommand = new BatchAddSymbolsCommand(batch);
                    sendBinaryDataCommandSync(batchAddSymbolsCommand, 5000);
                });
    }

    public CoreSymbolSpecification toCoreSymbolSpecification(final GeneratorSymbolSpec genSpec) {

        // TODO tests generator will create required type?
        return new CoreSymbolSpecification(genSpec.getSymbolId());

    }


    // TODO change to sync
    public int sendMultiSymbolGenCommandsAsync(long c,
                                               final BufferReader commandsBuffer) {

//        log.debug("FILL: \n{}",
//                PrintBufferUtil.prettyHexDump(commandsBuffer.getBuffer(), commandsBuffer.getInitialPosition(), 1000));


        commandsBuffer.reset();

        int commandsNum = 0;
        long timestamp = System.nanoTime();
        while (commandsBuffer.getRemainingSize() > 0) {
            commandsNum++;
            final byte cmd = commandsBuffer.readByte();
            final int symbolId = commandsBuffer.readInt();
            final long correlationId = 0xF00L + (c++ << 16);

//            log.debug("Sending command  cmd={} symbolId={} correlationId={}", cmd, symbolId, correlationId);

            clusterClient.placePreparedCommandMultiSymAsync(
                    correlationId,
                    timestamp++,
                    symbolId,
                    cmd,
                    commandsBuffer);
        }


        return commandsNum;
    }

    public void sendCommandsAsync(long c, int symbolId, final BufferReader commandsBuffer) {
        long timestamp = System.nanoTime();
        while (commandsBuffer.getRemainingSize() > 0) {

            clusterClient.placePreparedCommandAsync(
                    0xF00L + (c++ << 16),
                    timestamp++,
                    symbolId,
                    commandsBuffer);

        }
    }

    public void sendResetAsync() {
        clusterClient.sendNoArgsCommandAsync(1, 0, ExchangeCommandCode.RESET);
    }

    public float benchmarkMtps(final BufferReader apiCommandsBenchmark) {
        final long tStart = System.currentTimeMillis();
        final int commandsNum = sendMultiSymbolGenCommandsAsync(0L, apiCommandsBenchmark);
        final long tDuration = System.currentTimeMillis() - tStart;
        return commandsNum / (float) tDuration / 1000.0f;
    }

    /**
     * Run test using threads factory.
     * This is needed for correct cpu pinning.
     *
     * @param test - test lambda
     * @param <V>  return parameter type
     * @return result from test lambda
     */
    public <V> V executeTestingThread(final Callable<V> test) {
        try {
            final ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);
            final V result = executor.submit(test).get();
            executor.shutdown();
            executor.awaitTermination(3000, TimeUnit.SECONDS);
            return result;
        } catch (ExecutionException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public float executeTestingThreadPerfMtps(final Callable<Integer> test) {
        return executeTestingThread(() -> {
            final long tStart = System.currentTimeMillis();
            final int numMessages = test.call();
            final long tDuration = System.currentTimeMillis() - tStart;
            return numMessages / (float) tDuration / 1000.0f;
        });
    }


}
