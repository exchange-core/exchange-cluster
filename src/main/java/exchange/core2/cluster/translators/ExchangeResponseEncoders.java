package exchange.core2.cluster.translators;

import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static exchange.core2.cluster.utils.SerializationUtils.serializeL2MarketData;
import static exchange.core2.core.common.cmd.CommandResultCode.SUCCESS;
import static exchange.core2.core.common.cmd.OrderCommandType.ORDER_BOOK_REQUEST;

public class ExchangeResponseEncoders {

    static Map<OrderCommandType, ExchangeResponseEncoder> exchangeResponseEncoders = new ConcurrentHashMap<>();

    public static void encode(OrderCommand exchangeResponse, MutableDirectBuffer buffer, int offset) {
        int currentOffset = encodeResultCode(exchangeResponse.resultCode, buffer, offset);
        ExchangeResponseEncoder responseTranslator = exchangeResponseEncoders.get(exchangeResponse.command);
        if (responseTranslator != null) {
            responseTranslator.encode(exchangeResponse, buffer, currentOffset);
        }
    }

    static int encodeResultCode(CommandResultCode resultCode, MutableDirectBuffer buffer, int offset) {
        buffer.putInt(offset, resultCode.getCode());
        return offset + BitUtil.SIZE_OF_INT;
    }

    static ExchangeResponseEncoder orderBookRequestEncoder = ((exchangeResponse, buffer, offset) -> {
        if (exchangeResponse.resultCode == SUCCESS) {
            serializeL2MarketData(exchangeResponse.marketData, buffer, offset);
        }
    });

    static {
        exchangeResponseEncoders.put(ORDER_BOOK_REQUEST, orderBookRequestEncoder);
    }
}
