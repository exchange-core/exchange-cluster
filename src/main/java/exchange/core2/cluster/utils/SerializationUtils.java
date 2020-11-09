package exchange.core2.cluster.utils;

import exchange.core2.orderbook.L2MarketData;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.eclipse.collections.api.map.primitive.MutableIntLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.primitives.Longs.fromByteArray;
import static com.google.common.primitives.Longs.toByteArray;
import static java.lang.System.arraycopy;
import static java.util.Arrays.copyOfRange;

public class SerializationUtils {

    public static L2MarketData parseL2MarketData(MutableDirectBuffer buffer, int offset) {
        int currentOffset = offset;

        int askLength = buffer.getInt(currentOffset);
        currentOffset += BitUtil.SIZE_OF_INT;
        byte[] askPricesBytes = new byte[askLength];
        buffer.getBytes(currentOffset, askPricesBytes);
        long[] askPrices = bytesToLongs(askPricesBytes);
        currentOffset += askLength;

        byte[] askVolumesBytes = new byte[askLength];
        buffer.getBytes(currentOffset, askVolumesBytes);
        long[] askVolumes = bytesToLongs(askVolumesBytes);
        currentOffset += askLength;

        int askOrdersLength = buffer.getInt(currentOffset);
        currentOffset += BitUtil.SIZE_OF_INT;
        byte[] askOrdersBytes = new byte[askOrdersLength];
        buffer.getBytes(currentOffset, askOrdersBytes);
        long[] askOrders = bytesToLongs(askOrdersBytes);
        currentOffset += askOrdersLength;


        int bidLength = buffer.getInt(currentOffset);
        currentOffset += BitUtil.SIZE_OF_INT;
        byte[] bidPricesBytes = new byte[bidLength];
        buffer.getBytes(currentOffset, bidPricesBytes);
        long[] bidPrices = bytesToLongs(bidPricesBytes);
        currentOffset += bidLength;

        byte[] bidVolumesBytes = new byte[bidLength];
        buffer.getBytes(currentOffset, bidVolumesBytes);
        long[] bidVolumes = bytesToLongs(bidVolumesBytes);
        currentOffset += bidLength;

        int bidOrdersLength = buffer.getInt(currentOffset);
        currentOffset += BitUtil.SIZE_OF_INT;
        byte[] bidOrdersBytes = new byte[bidOrdersLength];
        buffer.getBytes(currentOffset, bidOrdersBytes);
        long[] bidOrders = bytesToLongs(bidOrdersBytes);

        return new L2MarketData(askPrices, askVolumes, askOrders, bidPrices, bidVolumes, bidOrders);
    }

    static byte[] longsToBytes(long[] longs) {
        byte[] bytes = new byte[longs.length * 8];
        int currentBytesLength = 0;
        for (long l : longs) {
            byte[] longBytes = toByteArray(l);
            arraycopy(longBytes, 0, bytes, currentBytesLength, 8);
            currentBytesLength += 8;
        }
        return bytes;
    }

    static long[] bytesToLongs(byte[] bytes) {
        long[] longs = new long[bytes.length / 8];
        int currentBytesLength = 0;
        for (int i = 0; i < longs.length; ++i) {
            byte[] longBytes = copyOfRange(bytes, currentBytesLength, currentBytesLength + 8);
            longs[i] = fromByteArray(longBytes);
            currentBytesLength += 8;
        }
        return longs;
    }


    // ---- IntLongHashMap

    public static void marshallIntLongHashMap(final MutableIntLongMap hashMap, final BufferWriter bytes) {

        bytes.writeInt(hashMap.size());

        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeInt(k);
            bytes.writeLong(v);
        });
    }

    public static IntLongHashMap readIntLongHashMap(final BufferReader bytes) {

        int length = bytes.readInt();
        final IntLongHashMap hashMap = new IntLongHashMap(length);
        // TODO shuffle (? performance can be reduced if populating linearly)
        for (int i = 0; i < length; i++) {
            int k = bytes.readInt();
            long v = bytes.readLong();
            hashMap.put(k, v);
        }
        return hashMap;
    }


    // ---- LongObjectHashMap

    public static <T extends BufferWritable> void marshallLongHashMap(final LongObjectHashMap<T> hashMap, final BufferWriter bytes) {

        bytes.writeInt(hashMap.size());

        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeLong(k);
            v.writeToBuffer(bytes);
        });
    }

    public static <T> void marshallLongHashMap(final LongObjectHashMap<T> hashMap, final BiConsumer<T, BufferWriter> valuesMarshaller, final BufferWriter bytes) {

        bytes.writeInt(hashMap.size());

        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeLong(k);
            valuesMarshaller.accept(v, bytes);
        });
    }

    public static <T> LongObjectHashMap<T> readLongHashMap(final BufferReader bytes, final Function<BufferReader, T> creator) {

        final int length = bytes.readInt();
        final LongObjectHashMap<T> hashMap = new LongObjectHashMap<>(length);
        for (int i = 0; i < length; i++) {
            hashMap.put(bytes.readLong(), creator.apply(bytes));
        }
        return hashMap;
    }

    // ---- IntObjectHashMap

    public static <T extends BufferWritable> void marshallIntHashMap(final IntObjectHashMap<T> hashMap, final BufferWriter bytes) {
        bytes.writeInt(hashMap.size());
        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeInt(k);
            v.writeToBuffer(bytes);
        });
    }

    public static <T> void marshallIntHashMap(final IntObjectHashMap<T> hashMap, final BufferWriter bytes, final Consumer<T> elementMarshaller) {
        bytes.writeInt(hashMap.size());
        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeInt(k);
            elementMarshaller.accept(v);
        });
    }


    public static <T> IntObjectHashMap<T> readIntHashMap(final BufferReader bytes, final Function<BufferReader, T> creator) {
        int length = bytes.readInt();
        final IntObjectHashMap<T> hashMap = new IntObjectHashMap<>(length);
        for (int i = 0; i < length; i++) {
            hashMap.put(bytes.readInt(), creator.apply(bytes));
        }
        return hashMap;
    }


}
