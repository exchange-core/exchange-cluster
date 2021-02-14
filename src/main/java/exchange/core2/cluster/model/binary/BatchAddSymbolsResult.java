package exchange.core2.cluster.model.binary;

import exchange.core2.orderbook.util.BufferReader;
import exchange.core2.orderbook.util.BufferWriter;

public final class BatchAddSymbolsResult implements BinaryDataResult {

    private final short resultCode;
    // TODO private final IntIntHashMap symbolResults;

    public BatchAddSymbolsResult(short resultCode) {
        this.resultCode = resultCode;
    }

    public BatchAddSymbolsResult(final BufferReader bytes) {
        resultCode = bytes.readShort();
    }

    @Override
    public short getResultCode() {
        return resultCode;
    }

    @Override
    public void writeToBuffer(BufferWriter buffer) {
        buffer.appendShort(resultCode);
    }

    @Override
    public String toString() {
        return "BatchAddSymbolsResult{" +
                "resultCode=" + resultCode +
                '}';
    }

    public enum AddSymbolResultCodes {
        SYMBOL_ALREADY_EXISTS((short)1);

        private final short code;

        AddSymbolResultCodes(short code) {
            this.code = code;
        }
    }
}
