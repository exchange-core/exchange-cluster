package exchange.core2.cluster.handlers;

import exchange.core2.cluster.model.CoreSymbolSpecification;
import exchange.core2.cluster.model.ExchangeCommandCode;
import exchange.core2.cluster.model.binary.*;
import exchange.core2.cluster.utils.BufferReader;
import exchange.core2.cluster.utils.BufferWriter;
import exchange.core2.orderbook.IOrder;
import exchange.core2.orderbook.IOrderBook;
import exchange.core2.orderbook.L2MarketData;
import exchange.core2.orderbook.OrderAction;
import exchange.core2.orderbook.naive.OrderBookNaiveImpl;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.PrintBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public final class MatchingEngine {

    private final static Logger log = LoggerFactory.getLogger(MatchingEngine.class);
    private final static EmptyOrderBook EMPTY_ORDER_BOOK = new EmptyOrderBook();

    private final Map<Integer, IOrderBook<CoreSymbolSpecification>> orderBooks;
    private final BufferWriter bufferWriter;

    public MatchingEngine(final BufferWriter bufferWriter) {
        this.orderBooks = new HashMap<>();
        this.bufferWriter = bufferWriter;
    }

    public void onMessage(final DirectBuffer buffer,
                          int offset,
                          int length) {


        // always read command
        final byte cmdCode = buffer.getByte(offset);
        offset += BitUtil.SIZE_OF_BYTE;
        length -= BitUtil.SIZE_OF_BYTE;

        // for some commands resolve symbolId to corresponding order book
        final IOrderBook<CoreSymbolSpecification> orderBook;
        if (ExchangeCommandCode.isOrderBookRelated(cmdCode)) {
            final int symbolId = buffer.getInt(offset);
            offset += BitUtil.SIZE_OF_INT;
            length -= BitUtil.SIZE_OF_INT;
            orderBook = orderBooks.get(symbolId);
        } else {
            orderBook = EMPTY_ORDER_BOOK;
        }

        final ExchangeCommandCode cmd = ExchangeCommandCode.fromCode(cmdCode);

        log.debug("Command {} orderBook={} offset={}", cmd, orderBook, offset);

        log.info("Data \n{}", PrintBufferUtil.prettyHexDump(buffer, offset, length));


        switch (cmd) {
            case PLACE_ORDER:
                orderBook.newOrder(buffer, offset);
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
                onBinaryCommand(buffer, offset, length);
                break;

            // TODO add more

            default:
                throw new UnsupportedOperationException("Unsupported command code " + cmd);
        }
    }


    private boolean addOrderBook(final CoreSymbolSpecification symbolSpecification) {

        final IOrderBook<CoreSymbolSpecification> orderBook = new OrderBookNaiveImpl<>(
                symbolSpecification, true, bufferWriter.getBuffer());

        log.debug("Created orderbook: {}", symbolSpecification.getSymbolId());

        // TODO check uniqueness
        IOrderBook<CoreSymbolSpecification> prev = orderBooks.putIfAbsent(symbolSpecification.getSymbolId(), orderBook);
        return prev == null;
    }

    private void onBinaryCommand(final DirectBuffer buffer,
                                 final int offset,
                                 int length) {

        // TODO use length
        final BufferReader bufferReader = new BufferReader(buffer, offset);
        final int binaryCommandCode = bufferReader.readInt();
        final BinaryCommandType binCmd = BinaryCommandType.of(binaryCommandCode);

        final BinaryDataResult result;
        switch (binCmd) {
            case ADD_SYMBOLS:
                final BatchAddSymbolsCommand batchAddSymbolsCommand = new BatchAddSymbolsCommand(bufferReader);
                batchAddSymbolsCommand.getSymbols().forEach(this::addOrderBook);
                result = new BatchAddSymbolsResult(0);

                break;
            case ADD_ACCOUNTS:
            default:
                result = new RuntimeErrorBinaryResult();
        }

        result.writeToBuffer(bufferWriter);

    }

    private void onBinaryQuery(final DirectBuffer buffer,
                               final int offset,
                               int length) {

    }

    private static class EmptyOrderBook implements IOrderBook<CoreSymbolSpecification> {

        @Override
        public void newOrder(DirectBuffer buffer, int offset) {
            throw new IllegalStateException();

        }

        @Override
        public void cancelOrder(DirectBuffer buffer, int offset) {
            throw new IllegalStateException();
        }

        @Override
        public void reduceOrder(DirectBuffer buffer, int offset) {
            throw new IllegalStateException();
        }

        @Override
        public void moveOrder(DirectBuffer buffer, int offset) {
            throw new IllegalStateException();
        }

        @Override
        public void sendL2Snapshot(DirectBuffer buffer, int offset) {
            throw new IllegalStateException();
        }

        @Override
        public int getOrdersNum(OrderAction action) {
            throw new IllegalStateException();
        }

        @Override
        public long getTotalOrdersVolume(OrderAction action) {
            throw new IllegalStateException();
        }

        @Override
        public IOrder getOrderById(long orderId) {
            throw new IllegalStateException();
        }

        @Override
        public void verifyInternalState() {
            throw new IllegalStateException();

        }

        @Override
        public List<IOrder> findUserOrders(long uid) {
            throw new IllegalStateException();
        }

        @Override
        public CoreSymbolSpecification getSymbolSpec() {
            throw new IllegalStateException();
        }

        @Override
        public Stream<? extends IOrder> askOrdersStream(boolean sorted) {
            throw new IllegalStateException();
        }

        @Override
        public Stream<? extends IOrder> bidOrdersStream(boolean sorted) {
            throw new IllegalStateException();
        }

        @Override
        public void fillAsks(int size, L2MarketData data) {

            throw new IllegalStateException();
        }

        @Override
        public void fillBids(int size, L2MarketData data) {
            throw new IllegalStateException();

        }

        @Override
        public int getTotalAskBuckets(int limit) {
            throw new IllegalStateException();
        }

        @Override
        public int getTotalBidBuckets(int limit) {
            throw new IllegalStateException();
        }
    }
}
