package exchange.core2.cluster.client;

import exchange.core2.core.common.cmd.OrderCommandType;
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

import java.io.File;
import java.util.List;

import static exchange.core2.cluster.ExchangeCoreClusterNode.calculatePort;
import static java.util.Collections.singletonList;

public class ExchangeCoreClusterClient implements EgressListener {

    private AeronCluster clusterClient;
    private final MediaDriver clientMediaDriver;
    private static final int CLIENT_FACING_PORT_OFFSET = 3;
    private final MutableDirectBuffer requestBuffer = new ExpandableDirectByteBuffer();
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusterClient.class);

    public ExchangeCoreClusterClient(String aeronDirName) {
        this.clientMediaDriver = MediaDriver.launch(
                new MediaDriver.Context()
                        .threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(false)
                        .errorHandler(Throwable::printStackTrace)
                        .aeronDirectoryName(aeronDirName)
        );
    }

    public void connectToCluster(String ingressHost, String egressHost, int egressPort) {
        String egressChannelEndpoint = egressHost + ":" + egressPort;
        this.clusterClient = AeronCluster.connect(
                new AeronCluster.Context()
                        .egressListener(this)
                        .egressChannel("aeron:udp?endpoint=" + egressChannelEndpoint)
                        .aeronDirectoryName(clientMediaDriver.aeronDirectoryName())
                        .ingressChannel("aeron:udp")
                        .ingressEndpoints(ingressEndpoints(singletonList(ingressHost))));
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
        log.info("Client message received: {}", buffer);
    }

    private void sendCreateUserRequest(long uid) {
        requestBuffer.putByte(0, OrderCommandType.ADD_USER.getCode());
        requestBuffer.putLong(BitUtil.SIZE_OF_BYTE, uid);
        log.info("Sending create user request: {}", requestBuffer);
        clusterClient.offer(requestBuffer, 0, BitUtil.SIZE_OF_BYTE + BitUtil.SIZE_OF_LONG);
    }

    public static String ingressEndpoints(final List<String> hostnames) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)  {
            sb.append(i).append('=');
            sb.append(hostnames.get(i)).append(':').append(calculatePort(i, CLIENT_FACING_PORT_OFFSET));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public static void main(String[] args) {
        final String aeronDirName = new File(System.getProperty("user.dir"), "aeron-cluster-client").getAbsolutePath();
        final String LOCALHOST = "localhost";
        final int egressPort = 19001;  // change for different clients

        ExchangeCoreClusterClient clusterClient = new ExchangeCoreClusterClient(aeronDirName);
        clusterClient.connectToCluster(LOCALHOST, LOCALHOST, egressPort);

        new Thread(() -> {
            while (true) clusterClient.pollEgress();
        }).start();

        clusterClient.sendCreateUserRequest(101);
        clusterClient.sendCreateUserRequest( 201);
    }
}
