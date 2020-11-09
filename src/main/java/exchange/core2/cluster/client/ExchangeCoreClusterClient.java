package exchange.core2.cluster.client;

import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.model.ExchangeCommandCode;
import exchange.core2.cluster.model.binary.BinaryDataCommand;
import exchange.core2.cluster.utils.BufferWriter;
import exchange.core2.cluster.utils.NetworkUtils;
import exchange.core2.orderbook.CommandsEncoder;
import exchange.core2.orderbook.IOrderBook;
import exchange.core2.orderbook.OrderAction;
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Single writer
 */
public class ExchangeCoreClusterClient implements EgressListener {

    private final AeronCluster aeronCluster;
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusterClient.class);

    private final MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
    private final BufferWriter bufferWriter = new BufferWriter(requestBuffer, 0);

    public ExchangeCoreClusterClient(final String aeronDirName,
                                     final ClusterConfiguration clusterConfiguration,
                                     final String egressChannelEndpoint,
                                     final boolean deleteOnStart) {

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

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            while (true) { // todo shutdown signal ?
                idleStrategy.idle(aeronCluster.pollEgress());
            }
        }); // TODO factory, not in constructor
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

        log.info("Received from cluster: \n{}", PrintBufferUtil.prettyHexDump(buffer, 0, length));

        long clientMessageId = buffer.getLong(offset);
        //responsesMap.put(clientMessageId, buffer);
    }


//    private DirectBuffer fetchExchangeResponse(long clientMessageId) {
//        DirectBuffer exchangeResponseBuffer;
//        while (true) {
//            aeronCluster.pollEgress();
//            exchangeResponseBuffer = responsesMap.remove(clientMessageId);
//            if (exchangeResponseBuffer != null) {
//                return exchangeResponseBuffer;
//            }
//        }
//    }

    public void placeOrder(final long clientMessageId,
                           final int symbolId,
                           final byte type,
                           final long orderId,
                           final long uid,
                           final long price,
                           final long reservedBidPrice,
                           final long size,
                           final OrderAction action) {

        int offset = 0;
        requestBuffer.putLong(offset, clientMessageId);
        offset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(BitUtil.SIZE_OF_LONG, ExchangeCommandCode.PLACE_ORDER.getCode());
        offset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(offset, symbolId);
        offset += BitUtil.SIZE_OF_INT;

        CommandsEncoder.placeOrder(
                requestBuffer,
                offset,
                type,
                orderId,
                uid,
                price,
                reservedBidPrice,
                size,
                action);
        offset += IOrderBook.PLACE_OFFSET_END;

        sendToCluster(requestBuffer, offset);
    }


    public void sendBinaryDataCommand(final long clientMessageId, final BinaryDataCommand binaryDataCommand) {

        bufferWriter.reset();
        bufferWriter.writeLong(clientMessageId);
        bufferWriter.writeByte(ExchangeCommandCode.BINARY_DATA_COMMAND.getCode());
        bufferWriter.writeInt(binaryDataCommand.getBinaryCommandTypeCode());
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

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, ExchangeCommandCode.ADD_CLIENT.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.info("Sending add user request: {}", requestBuffer);

        sendToCluster(requestBuffer, currentOffset);

        //return fetchExchangeResponse(clientMessageId);
    }


    public void sendResumeUserRequest(long uid) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, ExchangeCommandCode.RESUME_CLIENT.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.info("Sending resume user request: {}", requestBuffer);

        sendToCluster(requestBuffer, currentOffset);

        //return fetchExchangeResponse(clientMessageId);
    }

    public void sendSuspendUserRequest(long uid) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, ExchangeCommandCode.SUSPEND_CLIENT.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.info("Sending suspend user request: {}", requestBuffer);

        sendToCluster(requestBuffer, currentOffset);

        //return fetchExchangeResponse(clientMessageId);
    }

    public void sendBalanceAdjustmentRequest(long uid, int currency, long amount, long transactionId) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
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

        //return fetchExchangeResponse(clientMessageId);
    }


    public void sendOrderBookRequest(int symbol, int depth) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, ExchangeCommandCode.ORDER_BOOK_REQUEST.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putInt(currentOffset, symbol);
        currentOffset += BitUtil.SIZE_OF_INT;

        requestBuffer.putInt(currentOffset, depth);
        currentOffset += BitUtil.SIZE_OF_INT;

        log.info("Sending cancel order request: {}", requestBuffer);

        sendToCluster(requestBuffer, currentOffset);

        //return fetchExchangeResponse(clientMessageId);
    }
}
