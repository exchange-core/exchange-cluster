package exchange.core2.cluster.client;

import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.model.ExchangeCommandCode;
import exchange.core2.cluster.model.binary.BatchAddSymbolsResult;
import exchange.core2.cluster.model.binary.BinaryCommandType;
import exchange.core2.cluster.model.binary.BinaryDataCommand;
import exchange.core2.cluster.utils.NetworkUtils;
import exchange.core2.orderbook.IResponseHandler;
import exchange.core2.orderbook.OrderAction;
import exchange.core2.orderbook.util.BufferReader;
import exchange.core2.orderbook.util.BufferWriter;
import exchange.core2.orderbook.util.CommandsEncoder;
import exchange.core2.orderbook.util.ResponseFastDecoder;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.*;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single-threaded writer
 * TODO design detachable writer (one per writing thread)
 */
public class ExchangeCoreClusterClient implements EgressListener {

    private final AeronCluster aeronCluster;
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusterClient.class);

    private final MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
    private final BufferWriter bufferWriter = new BufferWriter(requestBuffer, 0);

    private final ResponseFastDecoder responseFastDecoder;

    private static final int ORDER_BOOK_RESPONSE_SHIFT = BitUtil.SIZE_OF_LONG; // clientMsgId only TODO +timestamp


    public ExchangeCoreClusterClient(final String aeronDirName,
                                     final ClusterConfiguration clusterConfiguration,
                                     final String egressChannelEndpoint,
                                     final IResponseHandler responseHandler,
                                     final boolean deleteOnStart) {

        this.responseFastDecoder = new ResponseFastDecoder(responseHandler);

        final MediaDriver clientMediaDriver = MediaDriver.launchEmbedded(
                new MediaDriver.Context()
                        .threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(deleteOnStart)
                        .errorHandler(Throwable::printStackTrace)
                        .aeronDirectoryName(aeronDirName)
        );

        final AeronCluster.Context clusterContext = new AeronCluster.Context()
                .egressListener(this) // TODO unsafe publishing
                .egressChannel("aeron:udp?endpoint=" + egressChannelEndpoint)
                .aeronDirectoryName(clientMediaDriver.aeronDirectoryName())
                .ingressChannel("aeron:udp")
                .ingressEndpoints(NetworkUtils.ingressEndpoints(clusterConfiguration));

        this.aeronCluster = AeronCluster.connect(clusterContext);
        log.info("Connected to cluster, starting poller...");

        // TODO factory, not in constructor
        final Thread thread = new Thread(() -> {
            while (true) { // todo shutdown signal ?
                idleStrategy.idle(aeronCluster.pollEgress());
            }
        });
        thread.setDaemon(true);
        thread.setName("client-poller");
        thread.start();

        log.info("Poller started");
    }

//    public void connectToCluster() {
//        this.aeronCluster = AeronCluster.connect(clusterContext);
//    }

    @Override
    public void onMessage(
            long clusterSessionId,
            long timestamp,
            DirectBuffer buffer,
            int offset,
            int length,
            Header header) {

        log.info("FROM CLUSTER <<< (t={} len={}): \n{}", timestamp, length, PrintBufferUtil.prettyHexDump(buffer, offset, length));

        final BufferReader bufferReader = new BufferReader(buffer, length, offset);

        final long correlationId = bufferReader.readLong();
        log.debug("correlationId={}", correlationId);

        if (ExchangeCommandCode.isOrderBookRelated(bufferReader.getByte(BitUtil.SIZE_OF_LONG))) {

            // user standard OrderBook responses decoder
            // TODO reuse buffer
            final BufferReader cmdReader = new BufferReader(
                    buffer,
                    length - ORDER_BOOK_RESPONSE_SHIFT,
                    offset + ORDER_BOOK_RESPONSE_SHIFT);

            // TODO read from msg
            final int symbolId = 55555;
            final long timestampCore = 1234567L;

            responseFastDecoder.readResult(cmdReader, timestampCore, correlationId, symbolId);

//            final OrderBookResponse orderBookResponse = ResponseDecoder.readResult(cmdReader);
//            log.debug("response: {}", orderBookResponse);

        } else {

            final byte commandCode = bufferReader.readByte();
            final ExchangeCommandCode exchangeCommandCode = ExchangeCommandCode.fromCode(commandCode);

            log.debug("exchangeCommandCode: {}", exchangeCommandCode);

            switch (exchangeCommandCode) {
                case BINARY_DATA_COMMAND:
                    binaryDataCommandResponse(bufferReader);
                    break;

                case BINARY_DATA_QUERY:

                default:
                    throw new IllegalStateException("not supported: " + exchangeCommandCode);
            }


        }


        // responsesMap.put(correlationId, buffer);
    }

    public void binaryDataCommandResponse(final BufferReader bufferReader) {

        log.info("binaryDataCommandResponse (read at={}):\n{}", bufferReader.getReadPosition(), bufferReader.prettyHexDump());

        short code = bufferReader.readShort();
        log.info("code={}", code);
        BinaryCommandType binaryCommandType = BinaryCommandType.of(code);
        switch (binaryCommandType) {
            case ADD_SYMBOLS:
                BatchAddSymbolsResult result = new BatchAddSymbolsResult(bufferReader);
                log.debug("result {}", result);
                break;
            default:
                throw new IllegalStateException("Unsupported " + binaryCommandType);
        }

    }


//    private DirectBuffer fetchExchangeResponse(long correlationId) {
//        DirectBuffer exchangeResponseBuffer;
//        while (true) {
//            aeronCluster.pollEgress();
//            exchangeResponseBuffer = responsesMap.remove(correlationId);
//            if (exchangeResponseBuffer != null) {
//                return exchangeResponseBuffer;
//            }
//        }
//    }

    public void placeOrderAsync(final long correlationId,
                                final long timestamp,
                                final int symbolId,
                                final byte type,
                                final long orderId,
                                final long uid,
                                final long price,
                                final long reservedBidPrice,
                                final long size,
                                final OrderAction action,
                                final int userCookie) {

        int offset = writeStandardCommandHeader(correlationId, timestamp, symbolId, ExchangeCommandCode.PLACE_ORDER);

        offset += CommandsEncoder.placeOrder(
                requestBuffer,
                offset,
                type,
                orderId,
                uid,
                price,
                reservedBidPrice,
                size,
                action,
                userCookie);

        sendToCluster(requestBuffer, offset);
    }


    public void cancelOrderAsync(final long correlationId,
                                 final long timestamp,
                                 final int symbolId,
                                 final long orderId,
                                 final long uid) {

        int offset = writeStandardCommandHeader(correlationId, timestamp, symbolId, ExchangeCommandCode.CANCEL_ORDER);

        offset += CommandsEncoder.cancel(
                requestBuffer,
                offset,
                orderId,
                uid);

        sendToCluster(requestBuffer, offset);
    }

    public void moveOrderAsync(final long correlationId,
                               final long timestamp,
                               final int symbolId,
                               final long orderId,
                               final long uid,
                               final long price) {

        int offset = writeStandardCommandHeader(correlationId, timestamp, symbolId, ExchangeCommandCode.MOVE_ORDER);

        offset += CommandsEncoder.move(
                requestBuffer,
                offset,
                orderId,
                uid,
                price);

        sendToCluster(requestBuffer, offset);
    }

    public void reduceOrderAsync(final long clientMessageId,
                                 final long timestamp,
                                 final int symbolId,
                                 final long orderId,
                                 final long uid,
                                 final long size) {

        int offset = writeStandardCommandHeader(clientMessageId, timestamp, symbolId, ExchangeCommandCode.REDUCE_ORDER);

        offset += CommandsEncoder.reduce(
                requestBuffer,
                offset,
                orderId,
                uid,
                size);

        sendToCluster(requestBuffer, offset);
    }

    public void sendL2DataQueryAsync(final long clientMessageId,
                                 final long timestamp,
                                 final int symbolId,
                                 final int numRecordsLimit) {

        int offset = writeStandardCommandHeader(clientMessageId, timestamp, symbolId, ExchangeCommandCode.ORDER_BOOK_REQUEST);

        offset += CommandsEncoder.L2DataQuery(
                requestBuffer,
                offset,
                numRecordsLimit);

        sendToCluster(requestBuffer, offset);
    }


    private int writeStandardCommandHeader(long correlationId,
                                           long timestamp,
                                           int symbolId,
                                           ExchangeCommandCode commandCode) {
        int offset = 0;
        requestBuffer.putLong(offset, correlationId);
        offset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putLong(offset, timestamp);
        offset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(offset, commandCode.getCode());
        offset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putInt(offset, symbolId);
        offset += BitUtil.SIZE_OF_INT;
        return offset;
    }


    public void sendBinaryDataCommand(final long correlationId,
                                      final long timestamp,
                                      final BinaryDataCommand binaryDataCommand) {

        bufferWriter.reset();
        bufferWriter.appendLong(correlationId);
        bufferWriter.appendLong(timestamp);
        bufferWriter.appendByte(ExchangeCommandCode.BINARY_DATA_COMMAND.getCode());
        bufferWriter.appendShort(binaryDataCommand.getBinaryCommandTypeCode());
        binaryDataCommand.writeToBuffer(bufferWriter);
        sendToCluster(requestBuffer, bufferWriter.getWriterPosition());
    }

    public void sendToCluster(final MutableDirectBuffer buffer, final int length) {

        log.info("Sending to cluster: \n{}", PrintBufferUtil.prettyHexDump(buffer, 0, length));

        while (aeronCluster.offer(buffer, 0, length) < 0) {
//            idleStrategy.idle(aeronCluster.pollEgress()); // TODO why poll?
            idleStrategy.idle(); // TODO why poll?
        }

        log.info("Published");
    }


    public void sendAddUserRequest(long uid) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long correlationId = System.nanoTime();
        requestBuffer.putLong(currentOffset, correlationId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, ExchangeCommandCode.ADD_CLIENT.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.info("Sending add user request: {}", requestBuffer);

        sendToCluster(requestBuffer, currentOffset);

        //return fetchExchangeResponse(correlationId);
    }


    public void sendResumeUserRequest(long uid) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long correlationId = System.nanoTime();
        requestBuffer.putLong(currentOffset, correlationId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, ExchangeCommandCode.RESUME_CLIENT.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.info("Sending resume user request: {}", requestBuffer);

        sendToCluster(requestBuffer, currentOffset);

        //return fetchExchangeResponse(correlationId);
    }

    public void sendSuspendUserRequest(long uid) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long correlationId = System.nanoTime();
        requestBuffer.putLong(currentOffset, correlationId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, ExchangeCommandCode.SUSPEND_CLIENT.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.info("Sending suspend user request: {}", requestBuffer);

        sendToCluster(requestBuffer, currentOffset);

        //return fetchExchangeResponse(correlationId);
    }

    public void sendBalanceAdjustmentRequest(long uid, int currency, long amount, long transactionId) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long correlationId = System.nanoTime();
        requestBuffer.putLong(currentOffset, correlationId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, ExchangeCommandCode.BALANCE_ADJUSTMENT.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putInt(currentOffset, currency);
        currentOffset += BitUtil.SIZE_OF_INT;

        requestBuffer.putLong(currentOffset, amount);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putLong(currentOffset, transactionId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.info("Sending balance adjustment request: {}", requestBuffer);

        sendToCluster(requestBuffer, currentOffset);

        //return fetchExchangeResponse(correlationId);
    }


    // TODO fix
    public void sendOrderBookRequest(int symbol, int depth) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long correlationId = System.nanoTime();
        requestBuffer.putLong(currentOffset, correlationId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, ExchangeCommandCode.ORDER_BOOK_REQUEST.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putInt(currentOffset, symbol);
        currentOffset += BitUtil.SIZE_OF_INT;

        requestBuffer.putInt(currentOffset, depth);
        currentOffset += BitUtil.SIZE_OF_INT;

        log.info("Sending cancel order request: {}", requestBuffer);

        sendToCluster(requestBuffer, currentOffset);

        //return fetchExchangeResponse(correlationId);
    }

    public void shutdown() {
        aeronCluster.close();
    }

}

