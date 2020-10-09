package exchange.core2.cluster.client;

import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.utils.NetworkUtils;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.cmd.OrderCommandType;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ExchangeCoreClusterClient implements EgressListener {

    private AeronCluster aeronCluster;
    private final AeronCluster.Context clusterContext;
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusterClient.class);
    private final Map<Long, DirectBuffer> responsesMap = new HashMap<>();

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

        this.clusterContext =  new AeronCluster.Context()
                .egressListener(this)
                .egressChannel("aeron:udp?endpoint=" + egressChannelEndpoint)
                .aeronDirectoryName(clientMediaDriver.aeronDirectoryName())
                .ingressChannel("aeron:udp")
                .ingressEndpoints(NetworkUtils.ingressEndpoints(clusterConfiguration));
    }

    public void connectToCluster() {
        this.aeronCluster = AeronCluster.connect(clusterContext);
    }

    @Override
    public void onMessage(
            long clusterSessionId,
            long timestamp,
            DirectBuffer buffer,
            int offset,
            int length,
            Header header
    ) {
        log.info("Client received message: {}", buffer);
        long clientMessageId = buffer.getLong(offset);
        responsesMap.put(clientMessageId, buffer);
    }


    private DirectBuffer fetchExchangeResponse(long clientMessageId) {
        DirectBuffer exchangeResponseBuffer;
        while (true) {
            aeronCluster.pollEgress();
            exchangeResponseBuffer = responsesMap.remove(clientMessageId);
            if (exchangeResponseBuffer != null) {
                return exchangeResponseBuffer;
            }
        }
    }

    protected DirectBuffer sendAddUserRequest(long uid) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, OrderCommandType.ADD_USER.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.info("Sending add user request: {}", requestBuffer);

        while (aeronCluster.offer(requestBuffer, 0, currentOffset) < 0) {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return fetchExchangeResponse(clientMessageId);
    }

    protected DirectBuffer sendResumeUserRequest(long uid) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, OrderCommandType.RESUME_USER.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.info("Sending resume user request: {}", requestBuffer);

        while (aeronCluster.offer(requestBuffer, 0, currentOffset) < 0) {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return fetchExchangeResponse(clientMessageId);
    }

    protected DirectBuffer sendSuspendUserRequest(long uid) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, OrderCommandType.SUSPEND_USER.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.info("Sending suspend user request: {}", requestBuffer);

        while (aeronCluster.offer(requestBuffer, 0, currentOffset) < 0) {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return fetchExchangeResponse(clientMessageId);
    }

    protected DirectBuffer sendBalanceAdjustmentRequest(long uid, int currency, long amount, long transactionId) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, OrderCommandType.BALANCE_ADJUSTMENT.getCode());
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

        while (aeronCluster.offer(requestBuffer, 0, currentOffset) < 0) {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return fetchExchangeResponse(clientMessageId);
    }

    protected DirectBuffer sendPlaceOrderRequest(
            long uid,
            long orderId,
            long price,
            long size,
            OrderAction orderAction,
            OrderType orderType,
            int symbol
    ) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, OrderCommandType.PLACE_ORDER.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putLong(currentOffset, orderId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putLong(currentOffset, price);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putLong(currentOffset, size);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, orderAction.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putByte(currentOffset, orderType.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putInt(currentOffset, symbol);
        currentOffset += BitUtil.SIZE_OF_INT;

        log.info("Sending place order request: {}", requestBuffer);

        while (aeronCluster.offer(requestBuffer, 0, currentOffset) < 0) {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return fetchExchangeResponse(clientMessageId);
    }

    protected DirectBuffer sendMoveOrderRequest(long uid, long orderId, long newPrice, int symbol) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, OrderCommandType.MOVE_ORDER.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putLong(currentOffset, orderId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putLong(currentOffset, newPrice);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putInt(currentOffset, symbol);
        currentOffset += BitUtil.SIZE_OF_INT;

        log.info("Sending move order request: {}", requestBuffer);

        while (aeronCluster.offer(requestBuffer, 0, currentOffset) < 0) {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return fetchExchangeResponse(clientMessageId);
    }

    protected DirectBuffer sendReduceOrderRequest(long uid, long orderId, long reduceSize, int symbol) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, OrderCommandType.REDUCE_ORDER.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putLong(currentOffset, orderId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putLong(currentOffset, reduceSize);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putInt(currentOffset, symbol);
        currentOffset += BitUtil.SIZE_OF_INT;

        log.info("Sending reduce order request: {}", requestBuffer);

        while (aeronCluster.offer(requestBuffer, 0, currentOffset) < 0) {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return fetchExchangeResponse(clientMessageId);
    }

    protected DirectBuffer sendCancelOrderRequest(long uid, long orderId, int symbol) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, OrderCommandType.CANCEL_ORDER.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putLong(currentOffset, orderId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putInt(currentOffset, symbol);
        currentOffset += BitUtil.SIZE_OF_INT;

        log.info("Sending cancel order request: {}", requestBuffer);

        while (aeronCluster.offer(requestBuffer, 0, currentOffset) < 0) {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return fetchExchangeResponse(clientMessageId);
    }

    protected DirectBuffer sendOrderBookRequest(int symbol, int depth) {
        MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
        int currentOffset = 0;

        long clientMessageId = System.nanoTime();
        requestBuffer.putLong(currentOffset, clientMessageId);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, OrderCommandType.ORDER_BOOK_REQUEST.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putInt(currentOffset, symbol);
        currentOffset += BitUtil.SIZE_OF_INT;

        requestBuffer.putInt(currentOffset, depth);
        currentOffset += BitUtil.SIZE_OF_INT;

        log.info("Sending cancel order request: {}", requestBuffer);

        while (aeronCluster.offer(requestBuffer, 0, currentOffset) < 0) {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        return fetchExchangeResponse(clientMessageId);
    }
}
