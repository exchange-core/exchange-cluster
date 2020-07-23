package exchange.core2.cluster.translators;

import exchange.core2.core.common.api.ApiCommand;
import org.agrona.DirectBuffer;

@FunctionalInterface
public interface ExchangeRequestDecoder {

    ApiCommand decode(DirectBuffer buffer, int offset);

}
