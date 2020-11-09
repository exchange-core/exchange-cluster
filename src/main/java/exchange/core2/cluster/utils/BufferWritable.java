package exchange.core2.cluster.utils;

import org.agrona.DirectBuffer;

public interface BufferWritable {

    void writeToBuffer(BufferWriter buffer);

}
