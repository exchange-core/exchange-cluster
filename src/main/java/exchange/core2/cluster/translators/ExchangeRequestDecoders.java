package exchange.core2.cluster.translators;

import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.cmd.OrderCommandType;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static exchange.core2.core.common.cmd.OrderCommandType.*;

public class ExchangeRequestDecoders {
    static Map<OrderCommandType, ExchangeRequestDecoder> exchangeRequestTranslators = new ConcurrentHashMap<>();

    public static ApiCommand decode(OrderCommandType cmdType, DirectBuffer buffer, int offset) {
        return exchangeRequestTranslators.get(cmdType).decode(buffer, offset);
    }

    static ExchangeRequestDecoder addUserRequestTranslator = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long userId---|
        long userId = buffer.getLong(offset);
        return ApiAddUser.builder()
                .uid(userId)
                .build();
    });

    static ExchangeRequestDecoder resumeUserRequestTranslator = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long userId---|
        long userId = buffer.getLong(offset);
        return ApiAddUser.builder()
                .uid(userId)
                .build();
    });

    static ExchangeRequestDecoder suspendUserRequestTranslator = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long userId---|
        long userId = buffer.getLong(offset);
        return ApiAddUser.builder()
                .uid(userId)
                .build();
    });


    static ExchangeRequestDecoder balanceAdjustmentRequestTranslator = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long userId---|---int productCode---|---long amount---|
        // |---long transactionId---|
        int productCodeOffset = offset + BitUtil.SIZE_OF_LONG;
        int amountOffset = productCodeOffset + BitUtil.SIZE_OF_INT;
        int transactionIdOffset = amountOffset + BitUtil.SIZE_OF_LONG;

        long userId = buffer.getLong(offset);
        int productCode = buffer.getInt(productCodeOffset);
        long amount = buffer.getLong(amountOffset);
        long transactionId = buffer.getLong(transactionIdOffset);
        return ApiAdjustUserBalance.builder()
                .uid(userId)
                .currency(productCode)
                .amount(amount)
                .transactionId(transactionId)
                .build();
    });

    static ExchangeRequestDecoder placeOrderRequestTranslator = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long userId---|---long orderId---|---long price---|
        // |---long size---|---byte orderAction---|---byte orderType---|---int productCode---|
        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int priceOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
        int sizeOffset = priceOffset + BitUtil.SIZE_OF_LONG;
        int orderActionOffset = sizeOffset + BitUtil.SIZE_OF_LONG;
        int orderTypeOffset = orderActionOffset + BitUtil.SIZE_OF_BYTE;
        int productCodeOffset = orderTypeOffset + BitUtil.SIZE_OF_BYTE;

        long userId = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        long price = buffer.getLong(priceOffset);
        long size = buffer.getLong(sizeOffset);
        OrderAction orderAction = OrderAction.of(buffer.getByte(orderActionOffset));
        OrderType orderType = OrderType.of(buffer.getByte(orderTypeOffset));
        int productCode = buffer.getInt(productCodeOffset);
        return ApiPlaceOrder.builder()
                .uid(userId)
                .orderId(orderId)
                .price(price)
                .size(size)
                .action(orderAction)
                .orderType(orderType)
                .symbol(productCode)
                .build();
    });

    static ExchangeRequestDecoder moveOrderRequestTranslator = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long userId---|---long orderId---|--long newPrice---|
        // |---int productCode---|
        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int newPriceOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
        int productCodeOffset = newPriceOffset + BitUtil.SIZE_OF_LONG;

        long userId = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        long newPrice = buffer.getLong(newPriceOffset);
        int productCode = buffer.getInt(productCodeOffset);
        return ApiMoveOrder.builder()
                .uid(userId)
                .orderId(orderId)
                .newPrice(newPrice)
                .symbol(productCode)
                .build();
    });

    static ExchangeRequestDecoder reduceOrderRequestTranslator = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long userId---|---long orderId---|---long reduceSize---|
        // |---int productCode---|
        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int reduceSizeOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
        int productCodeOffset = reduceSizeOffset + BitUtil.SIZE_OF_LONG;

        long userId = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        long reduceSize = buffer.getLong(reduceSizeOffset);
        int productCode = buffer.getInt(productCodeOffset);
        return ApiReduceOrder.builder()
                .uid(userId)
                .orderId(orderId)
                .reduceSize(reduceSize)
                .symbol(productCode)
                .build();
    });

    static ExchangeRequestDecoder cancelOrderRequestTranslator = ((buffer, offset) -> {
        // |---byte orderCommandType---|---long userId---|---long orderId---|---int productCode---|
        int orderIdOffset = offset + BitUtil.SIZE_OF_LONG;
        int productCodeOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;

        long userId = buffer.getLong(offset);
        long orderId = buffer.getLong(orderIdOffset);
        int productCode = buffer.getInt(productCodeOffset);
        return ApiMoveOrder.builder()
                .uid(userId)
                .orderId(orderId)
                .symbol(productCode)
                .build();
    });

    static ExchangeRequestDecoder orderBookRequestTranslator = ((buffer, offset) -> {
        // |---byte orderCommandType---|---int productCode---|---int depth---|
        int depthOffset = offset + BitUtil.SIZE_OF_INT;

        int productCode = buffer.getInt(offset);
        int depth = buffer.getInt(depthOffset);

        return ApiOrderBookRequest.builder()
                .symbol(productCode)
                .size(depth)
                .build();
    });

    static {
        exchangeRequestTranslators.put(ADD_USER, addUserRequestTranslator);
        exchangeRequestTranslators.put(RESUME_USER, resumeUserRequestTranslator);
        exchangeRequestTranslators.put(SUSPEND_USER, suspendUserRequestTranslator);
        exchangeRequestTranslators.put(BALANCE_ADJUSTMENT, balanceAdjustmentRequestTranslator);
        exchangeRequestTranslators.put(PLACE_ORDER, placeOrderRequestTranslator);
        exchangeRequestTranslators.put(MOVE_ORDER, moveOrderRequestTranslator);
        exchangeRequestTranslators.put(REDUCE_ORDER, reduceOrderRequestTranslator);
        exchangeRequestTranslators.put(CANCEL_ORDER, cancelOrderRequestTranslator);
        exchangeRequestTranslators.put(ORDER_BOOK_REQUEST, orderBookRequestTranslator);
    }
}
