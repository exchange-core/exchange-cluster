package exchange.core2.cluster.handlers;

import exchange.core2.cluster.model.CoreSymbolSpecification;
import exchange.core2.cluster.model.ExchangeCommandCode;
import exchange.core2.cluster.model.binary.*;
import exchange.core2.orderbook.IOrderBook;
import exchange.core2.orderbook.VoidOrderBookImpl;
import exchange.core2.orderbook.naive.OrderBookNaiveImpl;
import exchange.core2.orderbook.util.BufferReader;
import exchange.core2.orderbook.util.BufferWriter;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.PrintBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public final class MatchingEngine {

    private final static Logger log = LoggerFactory.getLogger(MatchingEngine.class);

    private final Map<Integer, IOrderBook<CoreSymbolSpecification>> orderBooks;
    private final BufferWriter bufferWriter;
    private final IOrderBook<CoreSymbolSpecification> emptyOrderBook;

    public MatchingEngine(final BufferWriter bufferWriter) {
        this.orderBooks = new HashMap<>();
        this.bufferWriter = bufferWriter;
        this.emptyOrderBook = new VoidOrderBookImpl<>(bufferWriter);
    }

    public void onMessage(final DirectBuffer buffer,
                          int offset,
                          int length) {

        // read timestamp
        final long timestamp = buffer.getLong(offset);
        offset += BitUtil.SIZE_OF_LONG;
        length -= BitUtil.SIZE_OF_LONG;

        // always read command
        final byte cmdCode = buffer.getByte(offset);
        offset += BitUtil.SIZE_OF_BYTE;
        length -= BitUtil.SIZE_OF_BYTE;

        // for some commands resolve symbolId to corresponding order book
        final IOrderBook<CoreSymbolSpecification> orderBook;
        final boolean orderBookRelated = ExchangeCommandCode.isOrderBookRelated(cmdCode);
        if (orderBookRelated) {
            final int symbolId = buffer.getInt(offset);
            log.debug("isOrderBookRelated symbolId={}", symbolId);
            offset += BitUtil.SIZE_OF_INT;
            length -= BitUtil.SIZE_OF_INT;
            orderBook = orderBooks.getOrDefault(symbolId, emptyOrderBook);

        } else {
            orderBook = emptyOrderBook;
        }

        final ExchangeCommandCode cmd = ExchangeCommandCode.fromCode(cmdCode);

        log.debug("Command {} orderBook={} offset={} timestamp={} ", cmd, orderBook, offset, timestamp);

        log.info("Data \n{}", PrintBufferUtil.prettyHexDump(buffer, offset, length));


        switch (cmd) {
            case PLACE_ORDER:
                orderBook.newOrder(buffer, offset, timestamp);
                break;

            case CANCEL_ORDER:
                orderBook.cancelOrder(buffer, offset);
                break;

            case MOVE_ORDER:
                orderBook.moveOrder(buffer, offset);
                break;

            case REDUCE_ORDER:
                orderBook.reduceOrder(buffer, offset);
                break;

            case ORDER_BOOK_REQUEST:
                orderBook.sendL2Snapshot(buffer, offset);
                break;

            case BINARY_DATA_QUERY:
                onBinaryQuery(buffer, offset, length);
                break;

            case BINARY_DATA_COMMAND:
                onBinaryCommand(buffer, cmdCode, offset, length);
                break;

            // TODO add more

            default:
                throw new UnsupportedOperationException("Unsupported command code " + cmd);
        }
    }


    private boolean addOrderBook(final CoreSymbolSpecification symbolSpecification) {

        final IOrderBook<CoreSymbolSpecification> orderBook = new OrderBookNaiveImpl<>(
                symbolSpecification, true, bufferWriter);

        log.debug("Created orderbook: {}", symbolSpecification.getSymbolId());

        // TODO check uniqueness
        IOrderBook<CoreSymbolSpecification> prev = orderBooks.putIfAbsent(symbolSpecification.getSymbolId(), orderBook);
        return prev == null;
    }

    private void onBinaryCommand(final DirectBuffer buffer,
                                 final byte cmdCode,
                                 final int offset,
                                 int length) {

        bufferWriter.appendByte(cmdCode);

//        log.info(">> BINARY COMMAND length={} offset={} \n{}",
//                length, offset,
//                PrintBufferUtil.prettyHexDump(buffer, offset, length));

        // TODO use length
        final BufferReader bufferReader = new BufferReader(buffer, length, offset);
        final short binaryCommandCode = bufferReader.readShort();



        log.info("offset={} len={} binaryCommandCode={}", offset, length, binaryCommandCode);
        final BinaryCommandType binCmd = BinaryCommandType.of(binaryCommandCode);

        final BinaryDataResult result;
        switch (binCmd) {
            case ADD_SYMBOLS:
                final BatchAddSymbolsCommand cmd = new BatchAddSymbolsCommand(bufferReader);
                bufferWriter.appendShort(cmd.getBinaryCommandTypeCode());
                cmd.getSymbols().forEach(this::addOrderBook);
                result = new BatchAddSymbolsResult(0);

                break;
            case ADD_ACCOUNTS:
            default:
                result = new RuntimeErrorBinaryResult();
        }

        // TODO try/catch exception, pack stacktrace into response, but avoid throwing exceptinons for regular errors

        result.writeToBuffer(bufferWriter);

    }

    private void onBinaryQuery(final DirectBuffer buffer,
                               final int offset,
                               int length) {

    }

}
