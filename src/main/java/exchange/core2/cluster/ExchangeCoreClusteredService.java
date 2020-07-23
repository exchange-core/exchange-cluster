package exchange.core2.cluster;

import exchange.core2.cluster.handlers.ExchangeRequestDecoders;
import exchange.core2.cluster.handlers.ExchangeResponseEncoders;
import exchange.core2.core.ExchangeApi;
import exchange.core2.core.ExchangeCore;
import exchange.core2.core.common.api.ApiCommand;
import exchange.core2.core.common.cmd.OrderCommand;
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


public class ExchangeCoreClusteredService implements ClusteredService {
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusteredService.class);

    private final MutableDirectBuffer egressMessageBuffer = new ExpandableDirectByteBuffer(512);
    private IdleStrategy idleStrategy;
    private final ExchangeCore exchangeCore = ExchangeCore.builder()
            .exchangeConfiguration(ExchangeConfiguration.defaultBuilder().build())
            .resultsConsumer((cmd, l) -> log.info("{}", cmd))
            .build();

    private final ExchangeApi exchangeApi = exchangeCore.getApi();


    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        log.info("Cluster service started");
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

        int currentOffset = offset;
        long clientSlug = buffer.getLong(currentOffset);
        currentOffset += BitUtil.SIZE_OF_LONG;

        OrderCommandType orderCommandType = OrderCommandType.fromCode(buffer.getByte(currentOffset));
        currentOffset += BitUtil.SIZE_OF_BYTE;

        ApiCommand exchangeRequest = ExchangeRequestDecoders.decode(orderCommandType, buffer, currentOffset);

        if (exchangeRequest == null) {
            log.info("Translator for OrderCommandType {} has not been implemented yet. Skipping.", orderCommandType);
            return;
        }

        if (session != null) {
            // |---long clientSlug---|---byte responseType---|---int commandResult---|
            // |---(optional) byte[] responseBody---|
            CompletableFuture<OrderCommand> responseFuture = exchangeApi.submitCommandAsyncFullResponse(exchangeRequest);
            OrderCommand exchangeResponse;

            int currentEgressBufferOffset = 0;
            egressMessageBuffer.putLong(currentEgressBufferOffset, clientSlug);
            currentEgressBufferOffset += BitUtil.SIZE_OF_LONG;

            egressMessageBuffer.putByte(currentEgressBufferOffset, orderCommandType.getCode());
            currentEgressBufferOffset += BitUtil.SIZE_OF_BYTE;

            try {
                exchangeResponse = responseFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Exception while completing response future occurred. Returning", e);
                return;
            }

            ExchangeResponseEncoders.encode(exchangeResponse, egressMessageBuffer, currentEgressBufferOffset);
            log.info("Responding with {}", egressMessageBuffer);
            while (session.offer(egressMessageBuffer, 0, 512) < 0) {
                idleStrategy.idle();
            }
        }
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        log.info("In onTimerEvent. CorrelationId: {} Timestamp: {}", correlationId, timestamp);
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        log.info("In onTakeSnapshot: {}", snapshotPublication);
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        log.info("In onRoleChange: {}", newRole);
    }

    @Override
    public void onTerminate(Cluster cluster) {
        log.info("In onTerminate: {}", cluster);
    }
}
