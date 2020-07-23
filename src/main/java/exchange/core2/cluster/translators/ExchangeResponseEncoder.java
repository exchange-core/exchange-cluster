package exchange.core2.cluster.translators;

import exchange.core2.core.common.cmd.OrderCommand;
import org.agrona.MutableDirectBuffer;

@FunctionalInterface
public interface ExchangeResponseEncoder {

    void encode(OrderCommand exchangeResponse, MutableDirectBuffer buffer, int offset);

}
