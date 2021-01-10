package exchange.core2.cluster.model.binary;

import exchange.core2.orderbook.util.*;

public final class RuntimeErrorBinaryResult implements BinaryDataResult {

    @Override
    public int getResultCode() {
        return -1;
    }

    @Override
    public void writeToBuffer(BufferWriter buffer) {

    }
}
