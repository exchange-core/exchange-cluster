package exchange.core2.cluster.utils;

import exchange.core2.orderbook.util.BufferWriter;

public interface BufferWritable {

    void writeToBuffer(BufferWriter buffer);

}
