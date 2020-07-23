package exchange.core2.cluster.client;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static exchange.core2.cluster.utils.NetworkUtils.ingressEndpoints;
import static java.util.Collections.singletonList;

public class ExchangeCoreClusterClient implements EgressListener {

    private AeronCluster clusterClient;
    private final AeronCluster.Context clusterContext;
    private final MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusterClient.class);
    private final Map<Long, DirectBuffer> responsesMap = new ConcurrentHashMap<>();

    public ExchangeCoreClusterClient(String aeronDirName, String ingressHost, String egressHost, int egressPort) {
        MediaDriver clientMediaDriver = MediaDriver.launch(
                new MediaDriver.Context()
                        .threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(false)
                        .errorHandler(Throwable::printStackTrace)
                        .aeronDirectoryName(aeronDirName)
        );

        String egressChannelEndpoint = egressHost + ":" + egressPort;
        this.clusterContext =  new AeronCluster.Context()
                .egressListener(this)
                .egressChannel("aeron:udp?endpoint=" + egressChannelEndpoint)
                .aeronDirectoryName(clientMediaDriver.aeronDirectoryName())
                .ingressChannel("aeron:udp")
                .ingressEndpoints(ingressEndpoints(singletonList(ingressHost)));
    }

    public void connectToCluster() {
        this.clusterClient = AeronCluster.connect(clusterContext);
    }

    public void pollEgress() {
        clusterClient.pollEgress();
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
        long slug = buffer.getLong(offset);
        responsesMap.put(slug, buffer);
        log.info("Client message received: {}", buffer);
    }

    protected DirectBuffer sendCreateUserRequest(long uid) {
        int currentOffset = 0;

        long slug = System.nanoTime();
        requestBuffer.putLong(currentOffset, slug);
        currentOffset += BitUtil.SIZE_OF_LONG;

        requestBuffer.putByte(currentOffset, OrderCommandType.ADD_USER.getCode());
        currentOffset += BitUtil.SIZE_OF_BYTE;

        requestBuffer.putLong(currentOffset, uid);
        currentOffset += BitUtil.SIZE_OF_LONG;

        log.info("Sending create user request: {}", requestBuffer);
        clusterClient.offer(requestBuffer, 0, currentOffset);

        DirectBuffer exchangeResponseBuffer;
        while(true) {
            exchangeResponseBuffer = responsesMap.get(slug);
            if (exchangeResponseBuffer != null) {
                return exchangeResponseBuffer;
            }
        }
    }
}
