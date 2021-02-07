package exchange.core2.cluster.model.binary;

import exchange.core2.orderbook.util.BufferReader;
import exchange.core2.orderbook.util.BufferWriter;

public final class BatchAddSymbolsResult implements BinaryDataResult {

    private final int resultCode;
    // TODO private final IntIntHashMap symbolResults;

    public BatchAddSymbolsResult(int resultCode) {
        this.resultCode = resultCode;
    }

    public BatchAddSymbolsResult(final BufferReader bytes) {
        resultCode = bytes.readInt();
    }

    @Override
    public int getResultCode() {
        return resultCode;
    }

    @Override
    public void writeToBuffer(BufferWriter buffer) {
        buffer.appendInt(resultCode);
    }

    @Override
    public String toString() {
        return "BatchAddSymbolsResult{" +
                "resultCode=" + resultCode +
                '}';
    }

    public enum AddSymbolResultCodes {
        SYMBOL_ALREADY_EXISTS(1);

        private final int code;

        AddSymbolResultCodes(int code) {
            this.code = code;
        }
    }
}
