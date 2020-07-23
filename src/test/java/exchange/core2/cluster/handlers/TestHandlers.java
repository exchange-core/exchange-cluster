package exchange.core2.cluster.handlers;

import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.cmd.OrderCommandType;
import org.agrona.BitUtil;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestHandlers {

    @Test
    public void testAddUserRequestDecoder() {
        MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        OrderCommandType commandType = OrderCommandType.ADD_USER;

        int currentOffset = 0;
        final long uid = 1341234562345L;
        buffer.putLong(currentOffset, uid);

        ApiCommand result = ExchangeRequestDecoders.decode(commandType, buffer, currentOffset);
        ApiCommand expected = ApiAddUser.builder()
                .uid(uid)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testResumeUserRequestDecoder() {
        MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        OrderCommandType commandType = OrderCommandType.RESUME_USER;

        int currentOffset = 0;
        final long uid = 1341234562345L;
        buffer.putLong(currentOffset, uid);

        ApiCommand result = ExchangeRequestDecoders.decode(commandType, buffer, currentOffset);
        ApiCommand expected = ApiResumeUser.builder()
                .uid(uid)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testSuspendUserRequestDecoder() {
        MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        OrderCommandType commandType = OrderCommandType.SUSPEND_USER;

        int currentOffset = 0;
        final long uid = 1341234562345L;
        buffer.putLong(currentOffset, uid);

        ApiCommand result = ExchangeRequestDecoders.decode(commandType, buffer, currentOffset);
        ApiCommand expected = ApiSuspendUser.builder()
                .uid(uid)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testBalanceAdjustmentRequestDecoder() {
        MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        OrderCommandType commandType = OrderCommandType.BALANCE_ADJUSTMENT;

        int currentOffset = 0;

        final long uid = 1341234562345L;
        buffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        final int currencyCode = 101;
        buffer.putInt(currentOffset, currencyCode);
        currentOffset += BitUtil.SIZE_OF_INT;

        final long amount = 101010;
        buffer.putLong(currentOffset, amount);
        currentOffset += BitUtil.SIZE_OF_LONG;

        final long transactionId = 2341233348L;
        buffer.putLong(currentOffset, transactionId);

        ApiCommand result = ExchangeRequestDecoders.decode(commandType, buffer, 0);
        ApiCommand expected = ApiAdjustUserBalance.builder()
                .uid(uid)
                .currency(currencyCode)
                .amount(amount)
                .transactionId(transactionId)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testPlaceOrderRequestDecoder() {
        MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        OrderCommandType commandType = OrderCommandType.PLACE_ORDER;

        int currentOffset = 0;

        final long uid = 1341234562345L;
        buffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        final long orderId = 34514512341L;
        buffer.putLong(currentOffset, orderId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        final long price = 1000;
        buffer.putLong(currentOffset, price);
        currentOffset += BitUtil.SIZE_OF_LONG;

        final long size = 10000;
        buffer.putLong(currentOffset, size);
        currentOffset += BitUtil.SIZE_OF_LONG;

        OrderAction orderAction = OrderAction.BID;
        buffer.putByte(currentOffset, orderAction.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        OrderType orderType = OrderType.GTC;
        buffer.putByte(currentOffset, orderType.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        int symbolCode = 123;
        buffer.putInt(currentOffset, symbolCode);

        ApiCommand result = ExchangeRequestDecoders.decode(commandType, buffer, 0);
        ApiCommand expected = ApiPlaceOrder.builder()
                .uid(uid)
                .orderId(orderId)
                .price(price)
                .size(size)
                .action(orderAction)
                .orderType(orderType)
                .symbol(symbolCode)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testMoveOrderRequestDecoder() {
        MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        OrderCommandType commandType = OrderCommandType.MOVE_ORDER;

        int currentOffset = 0;

        final long uid = 1341234562345L;
        buffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        final long orderId = 34514512341L;
        buffer.putLong(currentOffset, orderId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        final long newPrice = 1000;
        buffer.putLong(currentOffset, newPrice);
        currentOffset += BitUtil.SIZE_OF_LONG;

        int symbolCode = 123;
        buffer.putInt(currentOffset, symbolCode);

        ApiCommand result = ExchangeRequestDecoders.decode(commandType, buffer, 0);
        ApiCommand expected = ApiMoveOrder.builder()
                .uid(uid)
                .orderId(orderId)
                .newPrice(newPrice)
                .symbol(symbolCode)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testCancelOrderRequestDecoder() {
        MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        OrderCommandType commandType = OrderCommandType.CANCEL_ORDER;

        int currentOffset = 0;

        final long uid = 1341234562345L;
        buffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        final long orderId = 34514512341L;
        buffer.putLong(currentOffset, orderId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        int symbolCode = 123;
        buffer.putInt(currentOffset, symbolCode);

        ApiCommand result = ExchangeRequestDecoders.decode(commandType, buffer, 0);
        ApiCommand expected = ApiCancelOrder.builder()
                .uid(uid)
                .orderId(orderId)
                .symbol(symbolCode)
                .build();

        assertEquals(result, expected);
    }

    @Test
    public void testOrderBookRequestDecoder() {
        MutableDirectBuffer buffer = new ExpandableDirectByteBuffer();
        OrderCommandType commandType = OrderCommandType.ORDER_BOOK_REQUEST;

        int currentOffset = 0;

        int symbolCode = 123;
        buffer.putInt(currentOffset, symbolCode);
        currentOffset += BitUtil.SIZE_OF_INT;

        int depth = 10;
        buffer.putInt(currentOffset, depth);

        ApiCommand result = ExchangeRequestDecoders.decode(commandType, buffer, 0);
        ApiCommand expected = ApiOrderBookRequest.builder()
                .symbol(symbolCode)
                .size(depth)
                .build();

        assertEquals(result, expected);
    }
}
