package exchange.core2.cluster.testing;

import exchange.core2.benchmarks.generator.GeneratorSymbolSpec;
import exchange.core2.benchmarks.generator.clients.ClientsCurrencyAccountsGenerator;
import exchange.core2.benchmarks.generator.currencies.CurrenciesGenerator;
import exchange.core2.benchmarks.generator.orders.MultiSymbolGenResult;
import exchange.core2.benchmarks.generator.orders.MultiSymbolOrdersGenerator;
import exchange.core2.benchmarks.generator.symbols.SymbolsGenerator;
import org.apache.commons.math3.util.Pair;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TestDataGenerationHelper {

    public static TestDataFutures initiateTestDataGeneration(final TestDataParameters parameters,
                                                             final int seed) {

        // initiate currencies generation (very fast)
        final CompletableFuture<Map<Integer, Double>> currenciesFuture = CompletableFuture.supplyAsync(
                () -> CurrenciesGenerator.randomCurrencies(parameters.getNumCurrencies(), 1, seed));

        // initiate client accounts generation (fast) - depends on currencies
        final CompletableFuture<List<BitSet>> clientAccountsFuture = currenciesFuture.thenApplyAsync(
                curr -> ClientsCurrencyAccountsGenerator.generateClients(
                        parameters.getNumAccounts(),
                        curr,
                        seed));


        // initiate symbols generation (quite fast) - depends on currencies
        final CompletableFuture<List<Pair<GeneratorSymbolSpec, Double>>> symbolSpecsFuture = currenciesFuture.thenApplyAsync(
                curr -> SymbolsGenerator.generateRandomSymbols(
                        parameters.getNumSymbols(),
                        curr,
                        parameters.getAllowedSymbolTypes(),
                        40000,
                        seed));

        // initiate symbols generation (slowest) - depends on symbol specs and currencies
        final CompletableFuture<MultiSymbolGenResult> genResultFuture = symbolSpecsFuture.thenCombineAsync(
                clientAccountsFuture,
                (symSpec, clientAcc) -> MultiSymbolOrdersGenerator.generateMultipleSymbols(
                        symSpec,
                        parameters.getTotalTransactionsNumber(),
                        clientAcc,
                        parameters.getTargetOrderBookOrdersTotal(),
                        seed,
                        parameters.isAvalancheIOC()));

        return new TestDataFutures(
                currenciesFuture,
                symbolSpecsFuture,
                clientAccountsFuture,
                genResultFuture);
    }


}
