package exchange.core2.cluster.model;

import exchange.core2.cluster.utils.BufferReader;
import exchange.core2.cluster.utils.BufferWritable;
import exchange.core2.cluster.utils.BufferWriter;
import exchange.core2.orderbook.ISymbolSpecification;

import java.util.Objects;

public final class CoreSymbolSpecification implements ISymbolSpecification, BufferWritable {

    private final int symbolId;

    public CoreSymbolSpecification(int symbolId) {
        this.symbolId = symbolId;
    }


    public CoreSymbolSpecification(final BufferReader bytes) {
        this.symbolId = bytes.readInt();
    }


    public int getSymbolId() {
        return symbolId;
    }

    @Override
    public boolean isExchangeType() {
        return true;
    }

    @Override
    public int stateHash() {
        return Objects.hash(symbolId);
    }

    @Override
    public void writeToBuffer(BufferWriter buffer) {
        buffer.writeInt(symbolId);
    }
}
