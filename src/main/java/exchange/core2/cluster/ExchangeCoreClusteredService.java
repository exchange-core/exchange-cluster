package exchange.core2.cluster;

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.ExchangeCore;
import exchange.core2.core.common.L2MarketData;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.api.*;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.ExchangeConfiguration;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static exchange.core2.cluster.utils.SerializationUtils.serializeL2MarketData;


public class ExchangeCoreClusteredService implements ClusteredService {
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusteredService.class);

    private final MutableDirectBuffer egressMessageBuffer = new ExpandableDirectByteBuffer();
    private Cluster cluster;
    private IdleStrategy idleStrategy;
    private final ExchangeCore exchangeCore = ExchangeCore.builder()
            .exchangeConfiguration(ExchangeConfiguration.defaultBuilder().build())
            .resultsConsumer((cmd, l) -> log.info("{}", cmd))
            .build();

    private final ExchangeApi exchangeApi = exchangeCore.getApi();


    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        System.out.println("Cluster service started");
        this.cluster = cluster;
        this.idleStrategy = cluster.idleStrategy();
        exchangeCore.startup();
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {

    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {

    }

    @Override
    public void onSessionMessage(
            ClientSession session,
            long timestamp,
            DirectBuffer buffer,
            int offset,
            int length,
            Header header
    ) {
        log.info("Session message: {}", buffer);
        OrderCommandType orderCommandType = OrderCommandType.fromCode(buffer.getByte(offset));
        byte responseType = 0;
        switch (orderCommandType) {
            case ADD_USER: {
                // |---byte orderCommandType---|---long userId---|
                long userId = buffer.getLong(offset + BitUtil.SIZE_OF_BYTE);
                exchangeApi.submitCommandAsync(ApiAddUser.builder()
                        .uid(userId)
                        .build());

                break;
            }
            case RESUME_USER: {
                // |---byte orderCommandType---|---long userId---|
                long userId = buffer.getLong(offset + BitUtil.SIZE_OF_BYTE);
                exchangeApi.submitCommandAsync(ApiResumeUser.builder()
                        .uid(userId)
                        .build());

                break;
            }
            case SUSPEND_USER: {
                // |---byte orderCommandType---|---long userId---|
                long userId = buffer.getLong(offset + BitUtil.SIZE_OF_BYTE);
                exchangeApi.submitCommandAsync(ApiSuspendUser.builder()
                        .uid(userId)
                        .build());

                break;
            }
            case BALANCE_ADJUSTMENT: {
                // |---byte orderCommandType---|---long userId---|---int productCode---|---long amount---|
                // |---long transactionId---|
                int userIdOffset = offset + BitUtil.SIZE_OF_BYTE;
                int productCodeOffset = userIdOffset + BitUtil.SIZE_OF_LONG;
                int amountOffset = productCodeOffset + BitUtil.SIZE_OF_INT;
                int transactionIdOffset = amountOffset + BitUtil.SIZE_OF_LONG;

                long userId = buffer.getLong(userIdOffset);
                int productCode = buffer.getInt(productCodeOffset);
                long amount = buffer.getLong(amountOffset);
                long transactionId = buffer.getLong(transactionIdOffset);
                exchangeApi.submitCommandAsync(ApiAdjustUserBalance.builder()
                        .uid(userId)
                        .currency(productCode)
                        .amount(amount)
                        .transactionId(transactionId)
                        .build());

                break;
            }
            case PLACE_ORDER: {
                // |---byte orderCommandType---|---long userId---|---long orderId---|---long price---|
                // |---long size---|---byte orderAction---|---byte orderType---|---int productCode---|
                int userIdOffset = offset + BitUtil.SIZE_OF_BYTE;
                int orderIdOffset = userIdOffset + BitUtil.SIZE_OF_LONG;
                int priceOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
                int sizeOffset = priceOffset + BitUtil.SIZE_OF_LONG;
                int orderActionOffset = sizeOffset + BitUtil.SIZE_OF_LONG;
                int orderTypeOffset = orderActionOffset + BitUtil.SIZE_OF_BYTE;
                int productCodeOffset = orderTypeOffset + BitUtil.SIZE_OF_BYTE;

                long userId = buffer.getLong(userIdOffset);
                long orderId = buffer.getLong(orderIdOffset);
                long price = buffer.getLong(priceOffset);
                long size = buffer.getLong(sizeOffset);
                OrderAction orderAction = OrderAction.of(buffer.getByte(orderActionOffset));
                OrderType orderType = OrderType.of(buffer.getByte(orderTypeOffset));
                int productCode = buffer.getInt(productCodeOffset);
                exchangeApi.submitCommandAsync(ApiPlaceOrder.builder()
                        .uid(userId)
                        .orderId(orderId)
                        .price(price)
                        .size(size)
                        .action(orderAction)
                        .orderType(orderType)
                        .symbol(productCode)
                        .build());

                break;
            }
            case MOVE_ORDER: {
                // |---byte orderCommandType---|---long userId---|---long orderId---|--long newPrice---|
                // |---int productCode---|
                int userIdOffset = offset + BitUtil.SIZE_OF_BYTE;
                int orderIdOffset = userIdOffset + BitUtil.SIZE_OF_LONG;
                int newPriceOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
                int productCodeOffset = newPriceOffset + BitUtil.SIZE_OF_LONG;

                long userId = buffer.getLong(userIdOffset);
                long orderId = buffer.getLong(orderIdOffset);
                long newPrice = buffer.getLong(newPriceOffset);
                int productCode = buffer.getInt(productCodeOffset);
                exchangeApi.submitCommandAsync(ApiMoveOrder.builder()
                        .uid(userId)
                        .orderId(orderId)
                        .newPrice(newPrice)
                        .symbol(productCode)
                        .build());

                break;
            }
            case REDUCE_ORDER: {
                // |---byte orderCommandType---|---long userId---|---long orderId---|---long reduceSize---|
                // |---int productCode---|
                int userIdOffset = offset + BitUtil.SIZE_OF_BYTE;
                int orderIdOffset = userIdOffset + BitUtil.SIZE_OF_LONG;
                int reduceSizeOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;
                int productCodeOffset = reduceSizeOffset + BitUtil.SIZE_OF_LONG;

                long userId = buffer.getLong(userIdOffset);
                long orderId = buffer.getLong(orderIdOffset);
                long reduceSize = buffer.getLong(reduceSizeOffset);
                int productCode = buffer.getInt(productCodeOffset);
                exchangeApi.submitCommandAsync(ApiReduceOrder.builder()
                        .uid(userId)
                        .orderId(orderId)
                        .reduceSize(reduceSize)
                        .symbol(productCode)
                        .build());

                break;
            }
            case CANCEL_ORDER: {
                // |---byte orderCommandType---|---long userId---|---long orderId---|---int productCode---|
                int userIdOffset = offset + BitUtil.SIZE_OF_BYTE;
                int orderIdOffset = userIdOffset + BitUtil.SIZE_OF_LONG;
                int productCodeOffset = orderIdOffset + BitUtil.SIZE_OF_LONG;

                long userId = buffer.getLong(userIdOffset);
                long orderId = buffer.getLong(orderIdOffset);
                int productCode = buffer.getInt(productCodeOffset);
                exchangeApi.submitCommandAsync(ApiMoveOrder.builder()
                        .uid(userId)
                        .orderId(orderId)
                        .symbol(productCode)
                        .build());

                break;
            }
            case ORDER_BOOK_REQUEST: {
                // |---byte orderCommandType---|---int productCode---|---int depth---|
                int productCodeOffset = offset + BitUtil.SIZE_OF_BYTE;
                int depthOffset = productCodeOffset + BitUtil.SIZE_OF_INT;

                int productCode = buffer.getInt(productCodeOffset);
                int depth = buffer.getInt(depthOffset);

                responseType = OrderCommandType.ORDER_BOOK_REQUEST.getCode();
                egressMessageBuffer.putByte(0, responseType);
                try {
                    L2MarketData marketData = exchangeApi.requestOrderBookAsync(productCode, depth).get();
                    serializeL2MarketData(marketData, egressMessageBuffer, BitUtil.SIZE_OF_BYTE);
                } catch (InterruptedException | ExecutionException e) {
                    egressMessageBuffer.putByte(0, (byte) 0);
                    log.error("Exception occurred during L2MarketData serialization", e);
                }

                break;
            }
            default:
                log.info("Handler for command of type {} has not been implemented yet", orderCommandType);
                break;
        }

        if (session != null && responseType != 0) {
            // |---byte responseType---|---byte[] responseBody (variable length)---|
            log.info("Responding with {}", egressMessageBuffer);
            while (session.offer(egressMessageBuffer, 0, 8) < 0) {
                idleStrategy.idle();
            }
        }
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        System.out.println("In onTimerEvent: " + correlationId + " : " + timestamp);
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        System.out.println("In onTakeSnapshot: " + snapshotPublication);
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        System.out.println("In onRoleChange: " + newRole);
    }

    @Override
    public void onTerminate(Cluster cluster) {
        System.out.println("In onTerminate: " + cluster);
    }
}
