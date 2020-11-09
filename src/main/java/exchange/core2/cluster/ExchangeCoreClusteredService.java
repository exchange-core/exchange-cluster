package exchange.core2.cluster;

import exchange.core2.cluster.handlers.MatchingEngine;
import exchange.core2.cluster.utils.BufferWriter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.*;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


public class ExchangeCoreClusteredService implements ClusteredService {
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusteredService.class);

    private final MutableDirectBuffer egressMessageBuffer = new ExpandableDirectByteBuffer(512);
    private IdleStrategy idleStrategy;

    // clientMessageId is always written first, therefore set initial offset at SIZE_OF_LONG
    private final BufferWriter bufferWriter = new BufferWriter(egressMessageBuffer, BitUtil.SIZE_OF_LONG);

    private final MatchingEngine matchingEngine = new MatchingEngine(bufferWriter);

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        log.info("Cluster service started");
        this.idleStrategy = cluster.idleStrategy();
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        log.info("Session {} opened at {}", session, timestamp);
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        log.info("Session {} closed at {} of {}", session, timestamp, closeReason);
    }

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        log.info("Session message length={} offset={} \n{}",
                length, offset,
                PrintBufferUtil.prettyHexDump(buffer, offset, length));


        int currentOffset = offset;
        final long clientMessageId = buffer.getLong(currentOffset);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.debug("clientMessageId={}", clientMessageId);

        matchingEngine.onMessage(buffer, currentOffset, length);

        if (session != null) {
            // |---long clientMessageId---|---byte responseType---|---int commandResult---|
            // |---(optional) byte[] responseBody---|

            egressMessageBuffer.putLong(0, clientMessageId);

            final int writerPosition = bufferWriter.getWriterPosition();
            log.info("Responding with {}", egressMessageBuffer);
            while (session.offer(egressMessageBuffer, 0, writerPosition) < 0) {
                idleStrategy.idle();
            }
        }

        bufferWriter.reset();
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

    public void onNewLeadershipTermEvent(
            long leadershipTermId,
            long logPosition,
            long timestamp,
            long termBaseLogPosition,
            int leaderMemberId,
            int logSessionId,
            TimeUnit timeUnit,
            int appVersion) {
        log.info("onNewLeadershipTermEvent: leadershipTermId={} logPosition={} leaderMemberId={}", leadershipTermId, logPosition, leaderMemberId);
    }
}
