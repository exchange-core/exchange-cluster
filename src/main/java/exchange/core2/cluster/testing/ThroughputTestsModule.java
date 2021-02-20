/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.cluster.testing;

import exchange.core2.cluster.client.IgnoringResponseHandler;
import exchange.core2.orderbook.IResponseHandler;
import exchange.core2.orderbook.util.BufferReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;
import java.util.stream.IntStream;

public class ThroughputTestsModule {

    private static final Logger log = LoggerFactory.getLogger(ThroughputTestsModule.class);

    public static void throughputTestImpl(final TestDataParameters testDataParameters,
                                          final Function<IResponseHandler, TestContainer> testContainerFactory,
                                          final int iterations) {

        final TestDataFutures testDataFutures = TestDataGenerationHelper.initiateTestDataGeneration(testDataParameters, 1);

        final IResponseHandler responseHandler = new IgnoringResponseHandler();

        try (TestContainer container = testContainerFactory.apply(responseHandler)) {

            final TestingHelperClient client = container.getTestingHelperClient();

            final float avgMt = client.executeTestingThread(
                    () -> (float) IntStream.range(0, iterations)
                            .mapToObj(j -> {

                                client.loadSymbolsClientsAndPreFillOrders(testDataFutures);

                                final BufferReader commandsBenchmark = testDataFutures.getGenResult().join().getCommandsBenchmark().join();
                                final float perfMt = client.benchmarkMtps(commandsBenchmark);

                                log.info("{}. {} MT/s", j, String.format("%.3f", perfMt));

                                // TODO checks

                                // assertTrue(container.totalBalanceReport().isGlobalBalancesAllZero());

                                // compare orderBook final state just to make sure all commands executed same way
//                                testDataFutures.coreSymbolSpecifications.join().forEach(
//                                        symbol -> assertEquals(
//                                                testDataFutures.getGenResult().join().getGenResults().get(symbol.symbolId).getFinalOrderBookSnapshot(),
//                                                container.requestCurrentOrderBook(symbol.symbolId)));

                                // TODO compare events, balances, positions

                                client.sendResetAsync();

                                System.gc();

                                return perfMt;
                            })
                            .mapToDouble(x -> x)
                            .average().orElse(0));

            log.info("Average: {} MT/s", avgMt);
        }
    }

}
