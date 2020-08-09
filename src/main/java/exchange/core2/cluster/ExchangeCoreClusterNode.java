package exchange.core2.cluster;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static exchange.core2.cluster.utils.NetworkUtils.*;
import static java.util.stream.Collectors.toList;

public class ExchangeCoreClusterNode {

    private final ShutdownSignalBarrier barrier;
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusterNode.class);

    public ExchangeCoreClusterNode(ShutdownSignalBarrier barrier) {
        this.barrier = barrier;
    }

    private String udpChannel(final int nodeId, final String hostname, final int portOffset) {
        final int port = calculatePort(nodeId, portOffset);
        return new ChannelUriStringBuilder()
                .media("udp")
                .termLength(64 * 1024)
                .endpoint(hostname + ":" + port)
                .build();
    }

    private static String logControlChannel(final int nodeId, final String hostname, final int portOffset) {
        final int port = calculatePort(nodeId, portOffset);
        return new ChannelUriStringBuilder()
                .media("udp")
                .termLength(64 * 1024)
                .controlMode("manual")
                .controlEndpoint(hostname + ":" + port)
                .build();
    }

    public void start(final int nodeId, final int nNodes, final boolean deleteOnStart) {
        final String aeronDir = new File(System.getProperty("user.dir"), "aeron-cluster-node-" + nodeId)
                .getAbsolutePath();

        final String baseDir = new File(System.getProperty("user.dir"), "aeron-cluster-driver-" + nodeId)
                .getAbsolutePath();

        log.info("Aeron Dir = {}", aeronDir);
        log.info("Cluster Dir = {}", baseDir);

        MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context();
        Archive.Context archiveContext = new Archive.Context();
        AeronArchive.Context aeronArchiveContext = new AeronArchive.Context();
        ClusteredServiceContainer.Context serviceContainerContext = new ClusteredServiceContainer.Context();

        ExchangeCoreClusteredService service = new ExchangeCoreClusteredService();

        mediaDriverContext
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                .terminationHook(barrier::signal)
                .dirDeleteOnStart(deleteOnStart);

        archiveContext
                .archiveDir(new File(baseDir, "archive"))
                .controlChannel(udpChannel(nodeId, LOCALHOST, ARCHIVE_CONTROL_REQUEST_PORT_OFFSET))
                .localControlChannel("aeron:ipc?term-length=64k")
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED);

        aeronArchiveContext
                .lock(NoOpLock.INSTANCE)
                .controlRequestChannel(archiveContext.controlChannel())
                .controlRequestStreamId(archiveContext.controlStreamId())
                .controlResponseChannel(udpChannel(nodeId, LOCALHOST, ARCHIVE_CONTROL_RESPONSE_PORT_OFFSET))
                .aeronDirectoryName(aeronDir);

        consensusModuleContext
                .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(3600))
                .errorHandler(Throwable::printStackTrace)
                .clusterMemberId(nodeId)
                .clusterMembers(clusterMembers(IntStream.range(0, nNodes).mapToObj(i -> LOCALHOST).collect(toList())))
                .aeronDirectoryName(aeronDir)
                .clusterDir(new File(baseDir, "consensus-module"))
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel(logControlChannel(nodeId, LOCALHOST, LOG_CONTROL_PORT_OFFSET))
                .archiveContext(aeronArchiveContext.clone())
                .deleteDirOnStart(deleteOnStart);

        serviceContainerContext
                .aeronDirectoryName(aeronDir)
                .archiveContext(aeronArchiveContext.clone())
                .clusterDir(new File(baseDir, "service"))
                .clusteredService(service)
                .errorHandler(Throwable::printStackTrace);

        ClusteredMediaDriver clusteredMediaDriver = ClusteredMediaDriver.launch(
                mediaDriverContext,
                archiveContext,
                consensusModuleContext
        );

        ClusteredServiceContainer container = ClusteredServiceContainer.launch(serviceContainerContext);
    }
}
