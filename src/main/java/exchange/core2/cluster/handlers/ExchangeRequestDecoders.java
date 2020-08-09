package exchange.core2.cluster.handlers;

import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.cmd.OrderCommandType;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

import java.util.HashMap;
import java.util.Map;

import static exchange.core2.core.common.cmd.OrderCommandType.*;

public class ExchangeRequestDecoders {
    // TODO: switch - no need for a map
    static Map<OrderCommandType, ExchangeRequestDecoder> exchangeRequestDecoders = new HashMap<>();

    public static ApiCommand decode(OrderCommandType cmdType, DirectBuffer buffer, int offset) {
        return exchangeRequestDecoders.get(cmdType).decode(buffer, offset);
    }

    static ExchangeRequestDecoder addUserRequestDecoder = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long uid---|
        long uid = buffer.getLong(offset);

        return ApiAddUser.builder()
                .uid(uid)
                .build();
    });

    static ExchangeRequestDecoder resumeUserRequestDecoder = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long uid---|
        long uid = buffer.getLong(offset);

        return ApiResumeUser.builder()
                .uid(uid)
                .build();
    });

    static ExchangeRequestDecoder suspendUserRequestDecoder = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long uid---|
        long uid = buffer.getLong(offset);

        return ApiSuspendUser.builder()
                .uid(uid)
                .build();
    });


    static ExchangeRequestDecoder balanceAdjustmentRequestDecoder = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long uid---|---int productCode---|---long amount---|
        // |---long transactionId---|
        int currencyOffset = offset + BitUtil.SIZE_OF_LONG;
        int amountOffset = currencyOffset + BitUtil.SIZE_OF_INT;
        int transactionIdOffset = amountOffset + BitUtil.SIZE_OF_LONG;

        long uid = buffer.getLong(offset);
        int currency = buffer.getInt(currencyOffset);
        long amount = buffer.getLong(amountOffset);
        long transactionId = buffer.getLong(transactionIdOffset);

        return ApiAdjustUserBalance.builder()
                .uid(uid)
                .currency(currency)
                .amount(amount)
                .transactionId(transactionId)
                .build();
    });

    static ExchangeRequestDecoder placeOrderRequestDecoder = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long uid---|---long orderId---|---long price---|
        // |---long size---|---byte orderAction---|---byte orderType---|---int productCode---|
        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int priceOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
        int sizeOffset = priceOffset + BitUtil.SIZE_OF_LONG;
        int orderActionOffset = sizeOffset + BitUtil.SIZE_OF_LONG;
        int orderTypeOffset = orderActionOffset + BitUtil.SIZE_OF_BYTE;
        int symbolOffset = orderTypeOffset + BitUtil.SIZE_OF_BYTE;

        long uid = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        long price = buffer.getLong(priceOffset);
        long size = buffer.getLong(sizeOffset);
        OrderAction orderAction = OrderAction.of(buffer.getByte(orderActionOffset));
        OrderType orderType = OrderType.of(buffer.getByte(orderTypeOffset));
        int symbol = buffer.getInt(symbolOffset);

        return ApiPlaceOrder.builder()
                .uid(uid)
                .orderId(orderId)
                .price(price)
                .size(size)
                .action(orderAction)
                .orderType(orderType)
                .symbol(symbol)
                .build();
    });

    static ExchangeRequestDecoder moveOrderRequestDecoder = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long uid---|---long orderId---|---long newPrice---|
        // |---int productCode---|
        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int newPriceOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
        int symbolOffset = newPriceOffset + BitUtil.SIZE_OF_LONG;

        long uid = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        long newPrice = buffer.getLong(newPriceOffset);
        int symbol = buffer.getInt(symbolOffset);

        return ApiMoveOrder.builder()
                .uid(uid)
                .orderId(orderId)
                .newPrice(newPrice)
                .symbol(symbol)
                .build();
    });

    static ExchangeRequestDecoder reduceOrderRequestDecoder = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long uid---|---long orderId---|---long reduceSize---|
        // |---int productCode---|
        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int reduceSizeOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
        int symbolOffset = reduceSizeOffset + BitUtil.SIZE_OF_LONG;

        long uid = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        long reduceSize = buffer.getLong(reduceSizeOffset);
        int symbol = buffer.getInt(symbolOffset);

        return ApiReduceOrder.builder()
                .uid(uid)
                .orderId(orderId)
                .reduceSize(reduceSize)
                .symbol(symbol)
                .build();
    });

    static ExchangeRequestDecoder cancelOrderRequestDecoder = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long uid---|---long orderId---|---int productCode---|
        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int symbolCodeOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;

        long uid = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        int symbolCode = buffer.getInt(symbolCodeOffset);

        return ApiCancelOrder.builder()
                .uid(uid)
                .orderId(orderId)
                .symbol(symbolCode)
                .build();
    });

    static ExchangeRequestDecoder orderBookRequestDecoder = ((buffer, offset) -> {
        // |---byte orderCommandType---|---int productCode---|---int depth---|
        int depthOffset = offset + BitUtil.SIZE_OF_INT;

        int symbolCode = buffer.getInt(offset);
        int depth = buffer.getInt(depthOffset);

        return ApiOrderBookRequest.builder()
                .symbol(symbolCode)
                .size(depth)
                .build();
    });

    static {
        exchangeRequestDecoders.put(ADD_USER, addUserRequestDecoder);
        exchangeRequestDecoders.put(RESUME_USER, resumeUserRequestDecoder);
        exchangeRequestDecoders.put(SUSPEND_USER, suspendUserRequestDecoder);
        exchangeRequestDecoders.put(BALANCE_ADJUSTMENT, balanceAdjustmentRequestDecoder);
        exchangeRequestDecoders.put(PLACE_ORDER, placeOrderRequestDecoder);
        exchangeRequestDecoders.put(MOVE_ORDER, moveOrderRequestDecoder);
        exchangeRequestDecoders.put(REDUCE_ORDER, reduceOrderRequestDecoder);
        exchangeRequestDecoders.put(CANCEL_ORDER, cancelOrderRequestDecoder);
        exchangeRequestDecoders.put(ORDER_BOOK_REQUEST, orderBookRequestDecoder);
    }
}
