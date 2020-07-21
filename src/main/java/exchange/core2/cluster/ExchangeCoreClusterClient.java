package exchange.core2.cluster;

import io.aeron.CommonContext;
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

import java.util.List;

import static java.util.Collections.singletonList;

public class ExchangeCoreClusterClient implements EgressListener {

    private static AeronCluster clusterClient;
    private static final int CLIENT_FACING_PORT_OFFSET = 3;
    private static final String LOCALHOST = "localhost";

    private final MutableDirectBuffer createUserBuffer = new ExpandableDirectByteBuffer();
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusterClient.class);

    @Override
    public void onMessage(
            long clusterSessionId,
            long timestamp,
            DirectBuffer buffer,
            int offset,
            int length,
            Header header
    ) {
        log.info("Client message received: {}", buffer);
    }

    private void sendCreateUserRequest(AeronCluster cluster) {
        createUserBuffer.putInt(ExchangeCoreClusteredService.USER_ID_OFFSET, 234);
        log.info("Sending create user request: {}", createUserBuffer);
        cluster.offer(createUserBuffer, 0, BitUtil.SIZE_OF_INT);
    }

    public static String ingressEndpoints(final List<String> hostnames) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)  {
            sb.append(i).append('=');
            sb.append(hostnames.get(i)).append(':').append(ExchangeCoreClusterNode.calculatePort(i, CLIENT_FACING_PORT_OFFSET));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public static void main(String[] args) {
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-client";
        final int egressPort = 19001;  // change for different clients
        final ExchangeCoreClusterClient bookClusterClient = new ExchangeCoreClusterClient();

        MediaDriver clientMediaDriver = MediaDriver.launch(
                new MediaDriver.Context()
                        .threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(true)
                        .errorHandler(Throwable::printStackTrace)
                        .aeronDirectoryName(aeronDirName)
        );

        clusterClient = AeronCluster.connect(
                new AeronCluster.Context()
                        .egressListener(bookClusterClient)
                        .egressChannel("aeron:udp?endpoint=localhost:" + egressPort)
                        .aeronDirectoryName(clientMediaDriver.aeronDirectoryName())
                        .ingressChannel("aeron:udp")
                        .ingressEndpoints(ingressEndpoints(singletonList(LOCALHOST))));

        bookClusterClient.sendCreateUserRequest(clusterClient);
        while (true) clusterClient.pollEgress();
    }
}
