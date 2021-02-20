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

import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class ExchangeTestContainer {


    private AtomicLong uniqueIdCounterLong = new AtomicLong();
    private AtomicInteger uniqueIdCounterInt = new AtomicInteger();


    public static String timeBasedExchangeId() {
        return String.format("%012X", System.currentTimeMillis());
    }


//
//    public void sendBinaryDataCommandSync(final BinaryDataCommand data, final int timeOutMs) {
//        final Future<CommandResultCode> future = api.submitBinaryDataAsync(data);
//        try {
//            assertThat(future.get(timeOutMs, TimeUnit.MILLISECONDS), Is.is(CommandResultCode.SUCCESS));
//        } catch (final InterruptedException | ExecutionException | TimeoutException ex) {
//            log.error("Failed sending binary data command", ex);
//            throw new RuntimeException(ex);
//        }
//    }

    private int getRandomTransferId() {
        return uniqueIdCounterInt.incrementAndGet();
    }

    private long getRandomTransactionId() {
        return uniqueIdCounterLong.incrementAndGet();
    }

//    public final void userAccountsInit(List<BitSet> userCurrencies) {
//
//        // calculate max amount can transfer to each account so that it is not possible to get long overflow
//        final IntLongHashMap accountsNumPerCurrency = new IntLongHashMap();
//        userCurrencies.forEach(accounts -> accounts.stream().forEach(currency -> accountsNumPerCurrency.addToValue(currency, 1)));
//        final IntLongHashMap amountPerAccount = new IntLongHashMap();
//        accountsNumPerCurrency.forEachKeyValue((currency, numAcc) -> amountPerAccount.put(currency, Long.MAX_VALUE / (numAcc + 1)));
//        // amountPerAccount.forEachKeyValue((k, v) -> log.debug("{}={}", k, v));
//
//        createUserAccountsRegular(userCurrencies, amountPerAccount);
//    }

//
//    private void createUserAccountsRegular(List<BitSet> userCurrencies, IntLongHashMap amountPerAccount) {
//        final int numUsers = userCurrencies.size() - 1;
//
//        IntStream.rangeClosed(1, numUsers).forEach(uid -> {
//            api.submitCommand(ApiAddUser.builder().uid(uid).build());
//            userCurrencies.get(uid).stream().forEach(currency ->
//                    api.submitCommand(ApiAdjustUserBalance.builder()
//                            .uid(uid)
//                            .transactionId(getRandomTransactionId())
//                            .amount(amountPerAccount.get(currency))
//                            .currency(currency)
//                            .build()));
//        });
//
//        api.submitCommandAsync(ApiNop.builder().build()).join();
//    }
//
//    public void usersInit(int numUsers, Set<Integer> currencies) {
//
//        LongStream.rangeClosed(1, numUsers)
//                .forEach(uid -> {
//                    api.submitCommand(ApiAddUser.builder().uid(uid).build());
//                    long transactionId = 1L;
//                    for (int currency : currencies) {
//                        api.submitCommand(ApiAdjustUserBalance.builder()
//                                .uid(uid)
//                                .transactionId(transactionId++)
//                                .amount(10_0000_0000L)
//                                .currency(currency).build());
//                    }
//                });
//
//        api.submitCommandAsync(ApiNop.builder().build()).join();
//    }
//
//
//    public L2MarketData requestCurrentOrderBook(final int symbol) {
//        return api.requestOrderBookAsync(symbol, -1).join();
//    }
//
//    // todo rename
//    public void validateUserState(long uid, Consumer<SingleUserReportResult> resultValidator) throws InterruptedException, ExecutionException {
//        resultValidator.accept(getUserProfile(uid));
//    }
//
//    public SingleUserReportResult getUserProfile(long clientId) throws InterruptedException, ExecutionException {
//        return api.processReport(new SingleUserReportQuery(clientId), getRandomTransferId()).get();
//    }
//
//    public TotalCurrencyBalanceReportResult totalBalanceReport() {
//        final TotalCurrencyBalanceReportResult res = api.processReport(new TotalCurrencyBalanceReportQuery(), getRandomTransferId()).join();
//        final IntLongHashMap openInterestLong = res.getOpenInterestLong();
//        final IntLongHashMap openInterestShort = res.getOpenInterestShort();
//        final IntLongHashMap openInterestDiff = new IntLongHashMap(openInterestLong);
//        openInterestShort.forEachKeyValue((k, v) -> openInterestDiff.addToValue(k, -v));
//        if (openInterestDiff.anySatisfy(vol -> vol != 0)) {
//            throw new IllegalStateException("Open Interest balance check failed");
//        }
//
//        return res;
//    }
//
//    public int requestStateHash() throws InterruptedException, ExecutionException {
//        return api.processReport(new StateHashReportQuery(), getRandomTransferId()).get().getStateHash();
//    }


}
