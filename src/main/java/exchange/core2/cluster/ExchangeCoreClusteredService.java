package exchange.core2.cluster;

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.ExchangeCore;
import exchange.core2.core.common.config.ExchangeConfiguration;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExchangeCoreClusteredService implements ClusteredService {
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusteredService.class);

    public static int USER_ID_OFFSET = 0;

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
        int userId = buffer.getInt(offset + USER_ID_OFFSET);
        exchangeApi.createUser(userId, cmd -> log.info("Processed command {}", cmd));

        if (session != null) {
            egressMessageBuffer.putLong(0, 1);
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
