package exchange.core2.cluster.utils;

import exchange.core2.benchmarks.generator.GeneratorSymbolSpec;
import exchange.core2.benchmarks.generator.orders.MultiSymbolGenResult;
import org.apache.commons.math3.util.Pair;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class TestDataFutures {

    private final CompletableFuture<Map<Integer, Double>> currencies;
    private final CompletableFuture<List<Pair<GeneratorSymbolSpec, Double>>> symbolSpecs;
    private final CompletableFuture<List<BitSet>> clientAccounts;
    private final CompletableFuture<MultiSymbolGenResult> genResult;

    public TestDataFutures(final CompletableFuture<Map<Integer, Double>> currencies,
                           final CompletableFuture<List<Pair<GeneratorSymbolSpec, Double>>> symbolSpecs,
                           final CompletableFuture<List<BitSet>> clientAccounts,
                           final CompletableFuture<MultiSymbolGenResult> genResult) {

        this.currencies = currencies;
        this.symbolSpecs = symbolSpecs;
        this.clientAccounts = clientAccounts;
        this.genResult = genResult;
    }

    public CompletableFuture<Map<Integer, Double>> getCurrencies() {
        return currencies;
    }

    public CompletableFuture<List<Pair<GeneratorSymbolSpec, Double>>> getSymbolSpecs() {
        return symbolSpecs;
    }

    public CompletableFuture<List<BitSet>> getClientAccounts() {
        return clientAccounts;
    }

    public CompletableFuture<MultiSymbolGenResult> getGenResult() {
        return genResult;
    }
}
