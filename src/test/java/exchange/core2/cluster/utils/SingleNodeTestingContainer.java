package exchange.core2.cluster.utils;

import com.google.common.collect.Lists;
import exchange.core2.benchmarks.generator.GeneratorSymbolSpec;
import exchange.core2.benchmarks.generator.orders.MultiSymbolGenResult;
import exchange.core2.benchmarks.generator.util.ExecutionTime;
import exchange.core2.cluster.ExchangeCoreClusterNode;
import exchange.core2.cluster.GenericClusterIT;
import exchange.core2.cluster.client.ExchangeCoreClusterClient;
import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.conf.ClusterLocalConfiguration;
import exchange.core2.cluster.model.CoreSymbolSpecification;
import exchange.core2.cluster.model.ExchangeCommandCode;
import exchange.core2.cluster.model.binary.BatchAddSymbolsCommand;
import exchange.core2.cluster.model.binary.BinaryDataCommand;
import exchange.core2.cluster.model.binary.BinaryDataResult;
import exchange.core2.orderbook.IOrderBook;
import exchange.core2.orderbook.IResponseHandler;
import exchange.core2.orderbook.util.BufferReader;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.apache.commons.math3.util.Pair;
import org.hamcrest.core.Is;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;

public final class SingleNodeTestingContainer implements TestContainer {

    private static final Logger log = LoggerFactory.getLogger(GenericClusterIT.class);

    private final ExchangeCoreClusterClient clusterClient;
    private final ExchangeCoreClusterNode clusterNode;

    private final AffinityThreadFactory threadFactory = new AffinityThreadFactory(
            AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE);

    private static final String CLIENT_ENDPOINT_LOCAL_HOST = "localhost:19001";

    public static SingleNodeTestingContainer create(final IResponseHandler responseHandler) {

        log.info("Initializing cluster configuration...");
        final ClusterConfiguration clusterConfiguration = new ClusterLocalConfiguration(1);

        log.info("Created {}", clusterConfiguration);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ExchangeCoreClusterNode clusterNode = new ExchangeCoreClusterNode(barrier, clusterConfiguration);

        clusterNode.start(0, true);

        final String aeronDirName = new File(System.getProperty("client.dir"), "aeron-cluster-client").getAbsolutePath();

        final ExchangeCoreClusterClient clusterClient = new ExchangeCoreClusterClient(
                aeronDirName,
                clusterConfiguration,
                CLIENT_ENDPOINT_LOCAL_HOST,
                responseHandler,
                true,
                1);


        return new SingleNodeTestingContainer(clusterClient, clusterNode);
    }

    private SingleNodeTestingContainer(ExchangeCoreClusterClient clusterClient, ExchangeCoreClusterNode clusterNode) {
        this.clusterClient = clusterClient;
        this.clusterNode = clusterNode;
    }

    public ExchangeCoreClusterClient getClusterClient() {
        return clusterClient;
    }

    public ExchangeCoreClusterNode getClusterNode() {
        return clusterNode;
    }


    public void loadSymbolsClientsAndPreFillOrders(TestDataFutures testDataFutures) {

        // load symbols

        final List<Pair<GeneratorSymbolSpec, Double>> symbolSpecs = testDataFutures.getSymbolSpecs().join();

        log.info("Loading {} symbols...", symbolSpecs.size());
        try (ExecutionTime ignore = new ExecutionTime(t -> log.debug("Loaded all symbols in {}", t))) {
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
        log.info("Order books pre-fill with orders ({} KB)...", commandsFill.getSize() / 1024);

        try (ExecutionTime ignore = new ExecutionTime(t -> log.debug("Order books pre-fill completed in {}", t))) {
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

    public <R extends BinaryDataResult> void sendBinaryDataCommandSync(final BinaryDataCommand<R> data, final int timeOutMs) {
        final Future<R> future = clusterClient.sendCommandSync(data);
        try {
            assertThat(future.get(timeOutMs, TimeUnit.MILLISECONDS).getResultCode(), Is.is(IOrderBook.RESULT_SUCCESS));
        } catch (final InterruptedException | ExecutionException | TimeoutException ex) {
            log.error("Failed sending binary data command", ex);
            throw new RuntimeException(ex);
        }
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

    public float benchmarkMtps(final BufferReader apiCommandsBenchmark) {
        final long tStart = System.currentTimeMillis();
        final int commandsNum = sendMultiSymbolGenCommandsAsync(0L, apiCommandsBenchmark);
        final long tDuration = System.currentTimeMillis() - tStart;
        return commandsNum / (float) tDuration / 1000.0f;
    }

    @Override
    public void close() {

        clusterClient.shutdown();

        clusterNode.stop();

        //log.debug("await...");
        //barrier.await();
        //log.debug("after await");
    }
}
