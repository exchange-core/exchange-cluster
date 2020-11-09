package exchange.core2.cluster.utils;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;

public class BufferWriter {

    private final MutableDirectBuffer buffer;
    private final int initialPosition;
    private int writerPosition;

    public BufferWriter(final MutableDirectBuffer buffer, final int initialPosition) {

        this.buffer = buffer;
        this.initialPosition = initialPosition;
        this.writerPosition = initialPosition;
    }

    public MutableDirectBuffer getBuffer() {
        return buffer;
    }

    public int getInitialPosition() {
        return initialPosition;
    }

    public int getWriterPosition() {
        return writerPosition;
    }

    public void reset() {
        writerPosition = initialPosition;
    }

    public void writeByte(final byte b) {
        buffer.putByte(writerPosition, b);
        writerPosition += BitUtil.SIZE_OF_BYTE;
    }

    public void writeShort(final short s) {
        buffer.putShort(writerPosition, s);
        writerPosition += BitUtil.SIZE_OF_SHORT;
    }

    public void writeInt(final int i) {
        buffer.putInt(writerPosition, i);
        writerPosition += BitUtil.SIZE_OF_INT;
    }

    public void writeLong(final long w) {
        buffer.putLong(writerPosition, w);
        writerPosition += BitUtil.SIZE_OF_LONG;
    }

}
