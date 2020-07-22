package exchange.core2.cluster.utils;

import com.google.common.primitives.Longs;
import exchange.core2.core.common.L2MarketData;
import org.agrona.BitUtil;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.Arrays;

import static com.google.common.primitives.Longs.fromByteArray;
import static com.google.common.primitives.Longs.toByteArray;
import static java.lang.System.arraycopy;

public class SerializationUtils {
    public static void serializeL2MarketData(L2MarketData marketData, MutableDirectBuffer buffer, int offset) {
        int currentOffset = offset;

        byte[] askPrices = longsToBytes(marketData.askPrices);
        buffer.putInt(currentOffset, askPrices.length);
        currentOffset += BitUtil.SIZE_OF_INT;
        buffer.putBytes(currentOffset, askPrices);
        currentOffset += askPrices.length;

        byte[] askVolumes = longsToBytes(marketData.askVolumes);
        buffer.putBytes(currentOffset, askVolumes);
        currentOffset += askVolumes.length;

        byte[] askOrders = longsToBytes(marketData.askOrders);
        buffer.putInt(currentOffset, askOrders.length);
        currentOffset += BitUtil.SIZE_OF_INT;
        buffer.putBytes(currentOffset, askOrders);
        currentOffset += askOrders.length;

        byte[] bidPrices = longsToBytes(marketData.bidPrices);
        buffer.putInt(currentOffset, bidPrices.length);
        currentOffset += BitUtil.SIZE_OF_INT;
        buffer.putBytes(currentOffset, bidPrices);
        currentOffset += bidPrices.length;

        byte[] bidVolumes = longsToBytes(marketData.bidVolumes);
        buffer.putBytes(currentOffset, bidVolumes);
        currentOffset += bidVolumes.length;

        byte[] bidOrders = longsToBytes(marketData.bidOrders);
        buffer.putInt(currentOffset, bidOrders.length);
        currentOffset += BitUtil.SIZE_OF_INT;
        buffer.putBytes(currentOffset, bidOrders);
    }

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
            longs[i] = fromByteArray(Arrays.copyOfRange(bytes, currentBytesLength, currentBytesLength + 8));
            currentBytesLength += 8;
        }
        return longs;
    }
}
