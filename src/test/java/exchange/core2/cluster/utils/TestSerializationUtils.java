package exchange.core2.cluster.utils;

import org.junit.Test;

import static exchange.core2.cluster.utils.SerializationUtils.*;
import static org.junit.Assert.*;

public class TestSerializationUtils {

    @Test
    public void testLongsToBytes() {
        long[] longs = new long[]{100, 0, Long.MIN_VALUE, Long.MAX_VALUE, 10L};
        byte[] bytes = longsToBytes(longs);
        assertArrayEquals(new byte[]{
                0, 0, 0, 0, 0, 0, 0, 100,
                0, 0, 0, 0, 0, 0, 0, 0,
                -128, 0, 0, 0, 0, 0, 0, 0,
                127, -1, -1, -1, -1, -1, -1, -1,
                0, 0, 0, 0, 0, 0, 0, 10,
        }, bytes);
    }

    @Test
    public void testBytesToLongs() {
        byte[] bytes = new byte[]{
                0, 0, 0, 0, 0, 0, 0, 100,
                0, 0, 0, 0, 0, 0, 0, 0,
                -128, 0, 0, 0, 0, 0, 0, 0,
                127, -1, -1, -1, -1, -1, -1, -1,
                0, 0, 0, 0, 0, 0, 0, 10,
        };
        long[] longs = bytesToLongs(bytes);
        assertArrayEquals(new long[]{100, 0, Long.MIN_VALUE, Long.MAX_VALUE, 10L}, longs);
    }

//    @Test
//    public void testSerializeAndParseL2MarketData() {
//        L2MarketData originalMarketData = new L2MarketData(
//                new long[]{200, 300, 400},
//                new long[]{100, 150, 50},
//                new long[]{123412342514L, 123412355263554L, 1451236243521341L, 1452451432534L, 134123431234L},
//                new long[]{100, 120, 150},
//                new long[]{100, 10, 20},
//                new long[]{123412312514L, 123412255263554L, 1451246243521341L, 1452451412534L, 134123431232L}
//        );
//        MutableDirectBuffer buffer = new ExpandableDirectByteBuffer(8);
//        serializeL2MarketData(originalMarketData, buffer, 0);
//        L2MarketData parsedMarketData = parseL2MarketData(buffer, 0);
//        assertEquals(originalMarketData, parsedMarketData);
//    }
}
