package exchange.core2.cluster;

import exchange.core2.cluster.conf.AeronServiceType;
import exchange.core2.cluster.conf.ClusterConfiguration;
import exchange.core2.cluster.utils.NetworkUtils;
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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ExchangeCoreClusterNode {

    private final static Logger log = LoggerFactory.getLogger(ExchangeCoreClusterNode.class);

    private static final String ARCHIVE_REPLICATION_CHANNEL = "aeron:ipc";

    private final ShutdownSignalBarrier barrier;
    private final ClusterConfiguration clusterConfiguration;

    private final ThreadFactory threadFactory;

    // todo initialize with factory (creator)
    private ClusteredServiceContainer container;
    private ClusteredMediaDriver clusteredMediaDriver;

    public ExchangeCoreClusterNode(final ShutdownSignalBarrier barrier,
                                   final ClusterConfiguration clusterConfiguration,
                                   final ThreadFactory threadFactory) {
        this.barrier = barrier;
        this.clusterConfiguration = clusterConfiguration;
        this.threadFactory = threadFactory;
    }


    private String udpChannel(final int nodeId, final AeronServiceType aeronServiceType) {

        return new ChannelUriStringBuilder()
                .media("udp")
                .termLength(256 * 1024)
                .endpoint(clusterConfiguration.getNodeEndpoint(nodeId, aeronServiceType))
                .build();
    }

    private String logControlChannel(final int nodeId) {

        return new ChannelUriStringBuilder()
                .media("udp")
                .termLength(256 * 1024)
                .controlMode("manual") // TODO try dynamic
                .controlEndpoint(clusterConfiguration.getNodeEndpoint(nodeId, AeronServiceType.LOG_CONTROL))
                .build();
    }

    /**
     * Specify the replication channel. This channel is the one that the local archive for a node will receive replication responses from
     * other archives when the log or snapshot replication step is required. This was added in version 1.33.0 and is a required parameter.
     * It is important in a production environment that this channel’s endpoint is not set to localhost, but instead a hostname/ip address
     * that is reachable by the other nodes in the cluster.
     */
    private  String logReplicationChannel(final int nodeId) {
        return new ChannelUriStringBuilder()
            .media("udp")
            .endpoint(clusterConfiguration.getNodeEndpoint(nodeId, AeronServiceType.REPLICATION_CHANNEL))
            .build();
    }

    public void start(final int nodeId, final boolean deleteOnStart) {
        final String aeronDir = new File(System.getProperty("user.dir"), "aeron-cluster-node-" + nodeId)
                .getAbsolutePath();

        final String baseDir = new File(System.getProperty("user.dir"), "aeron-cluster-driver-" + nodeId)
                .getAbsolutePath();

        log.info("Aeron Dir = {}", aeronDir);
        log.info("Cluster Dir = {}", baseDir);

        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .conductorThreadFactory(threadFactory)
                .receiverThreadFactory(threadFactory)
                .senderThreadFactory(threadFactory)
                .sharedNetworkThreadFactory(threadFactory)
                .sharedThreadFactory(threadFactory)
                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                .terminationHook(() -> {
                    log.debug("terminationHook - SIGNAL...");
                    barrier.signal();
                })
                .dirDeleteOnStart(deleteOnStart);


        final Archive.Context archiveContext = new Archive.Context()
                .archiveDir(new File(baseDir, "archive"))
                .controlChannel(udpChannel(nodeId, AeronServiceType.ARCHIVE_CONTROL_REQUEST))
                .localControlChannel("aeron:ipc?term-length=256k")
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(deleteOnStart)
                .threadFactory(threadFactory)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .replicationChannel(ARCHIVE_REPLICATION_CHANNEL);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
                .lock(NoOpLock.INSTANCE)
                .controlRequestChannel(archiveContext.localControlChannel())
                .controlRequestStreamId(archiveContext.controlStreamId())
                .controlResponseChannel(archiveContext.localControlChannel())
                .aeronDirectoryName(aeronDir);


        final String clusterMembers = NetworkUtils.clusterMembers(clusterConfiguration);
        log.info("clusterMembers: {}", clusterMembers);

        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context();

        consensusModuleContext
                .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(3600))
                .errorHandler(Throwable::printStackTrace)
                .clusterMemberId(nodeId)
                .clusterMembers(clusterMembers)
                .aeronDirectoryName(aeronDir)
                .clusterDir(new File(baseDir, "consensus-module"))
                .ingressChannel("aeron:udp?term-length=256k")
                .logChannel(logControlChannel(nodeId))
                .replicationChannel(logReplicationChannel(nodeId))
                .archiveContext(aeronArchiveContext.clone())
                .threadFactory(threadFactory)
                .deleteDirOnStart(deleteOnStart);

        // TODO provide
        final ExchangeCoreClusteredService service = new ExchangeCoreClusteredService();

        final ClusteredServiceContainer.Context serviceContainerContext = new ClusteredServiceContainer.Context();

        serviceContainerContext
                .aeronDirectoryName(aeronDir)
                .archiveContext(aeronArchiveContext.clone())
                .clusterDir(new File(baseDir, "service"))
                .clusteredService(service)
                .threadFactory(threadFactory)
                .errorHandler(Throwable::printStackTrace);

        clusteredMediaDriver = ClusteredMediaDriver.launch(
                mediaDriverContext,
                archiveContext,
                consensusModuleContext);

        container = ClusteredServiceContainer.launch(serviceContainerContext);

    }

    public void stop() {

        log.debug("Closing ClusteredServiceContainer....");
        container.close();
        //CloseHelper.quietClose(container);

        log.debug("Closing ClusteredMediaDriver....");
        clusteredMediaDriver.close();
        //CloseHelper.quietClose(clusteredMediaDriver);

        log.debug("Cluster node closed");
    }
}
