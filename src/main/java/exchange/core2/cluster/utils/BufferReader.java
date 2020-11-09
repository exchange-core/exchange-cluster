package exchange.core2.cluster.utils;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

public class BufferReader {

    private final DirectBuffer buffer;
    private final int initialPosition;
    private int readPosition;

    // TODO limit reader position on creation

    public BufferReader(final DirectBuffer buffer, final int initialPosition) {

        this.buffer = buffer;
        this.initialPosition = initialPosition;
        this.readPosition = initialPosition;
    }

    public DirectBuffer getBuffer() {
        return buffer;
    }

    public int getInitialPosition() {
        return initialPosition;
    }

    public int getReadPosition() {
        return readPosition;
    }

    public byte readByte() {
        final byte b = buffer.getByte(readPosition);
        readPosition += BitUtil.SIZE_OF_BYTE;
        return b;
    }

    public short readShort() {
        final short s = buffer.getShort(readPosition);
        readPosition += BitUtil.SIZE_OF_SHORT;
        return s;
    }

    public int readInt() {
        final int i = buffer.getInt(readPosition);
        readPosition += BitUtil.SIZE_OF_INT;
        return i;
    }

    public long readLong() {
        final long w = buffer.getLong(readPosition);
        readPosition += BitUtil.SIZE_OF_LONG;
        return w;
    }

}
