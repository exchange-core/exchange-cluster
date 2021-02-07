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
import java.util.concurrent.TimeUnit;

import static exchange.core2.cluster.utils.NetworkUtils.clusterMembers;

public class ExchangeCoreClusterNode {

    private final ShutdownSignalBarrier barrier;
    private final ClusterConfiguration clusterConfiguration;
    private final Logger log = LoggerFactory.getLogger(ExchangeCoreClusterNode.class);

    // todo initialize with factory (creator)
    private ClusteredServiceContainer container;
    private ClusteredMediaDriver clusteredMediaDriver;

    public ExchangeCoreClusterNode(ShutdownSignalBarrier barrier, ClusterConfiguration clusterConfiguration) {
        this.barrier = barrier;
        this.clusterConfiguration = clusterConfiguration;
    }


    private String udpChannel(final int nodeId, final AeronServiceType aeronServiceType) {

        return new ChannelUriStringBuilder()
                .media("udp")
                .termLength(64 * 1024)
                .endpoint(clusterConfiguration.getNodeEndpoint(nodeId, aeronServiceType))
                .build();
    }

    private String logControlChannel(final int nodeId, final AeronServiceType aeronServiceType) {

        return new ChannelUriStringBuilder()
                .media("udp")
                .termLength(64 * 1024)
                .controlMode("manual")
                .controlEndpoint(clusterConfiguration.getNodeEndpoint(nodeId, aeronServiceType))
                .build();
    }

    public void start(final int nodeId, final boolean deleteOnStart) {
        final String aeronDir = new File(System.getProperty("user.dir"), "aeron-cluster-node-" + nodeId)
                .getAbsolutePath();

        final String baseDir = new File(System.getProperty("user.dir"), "aeron-cluster-driver-" + nodeId)
                .getAbsolutePath();

        log.info("Aeron Dir = {}", aeronDir);
        log.info("Cluster Dir = {}", baseDir);

        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();

        mediaDriverContext
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                .terminationHook(() -> {
                    log.debug("terminationHook - SIGNAL...");
                    barrier.signal();
                })
                .dirDeleteOnStart(deleteOnStart);


        final Archive.Context archiveContext = new Archive.Context();

        archiveContext
                .archiveDir(new File(baseDir, "archive"))
                .controlChannel(udpChannel(nodeId, AeronServiceType.ARCHIVE_CONTROL_REQUEST))
                .localControlChannel("aeron:ipc?term-length=64k")
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context();

        aeronArchiveContext
                .lock(NoOpLock.INSTANCE)
                .controlRequestChannel(archiveContext.controlChannel())
                .controlRequestStreamId(archiveContext.controlStreamId())
                .controlResponseChannel(udpChannel(nodeId, AeronServiceType.ARCHIVE_CONTROL_RESPONSE))
                .aeronDirectoryName(aeronDir);

        final String clusterMembers = NetworkUtils.clusterMembers(clusterConfiguration);

        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context();

        consensusModuleContext
                .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(3600))
                .errorHandler(Throwable::printStackTrace)
                .clusterMemberId(nodeId)
                .clusterMembers(clusterMembers)
                .aeronDirectoryName(aeronDir)
                .clusterDir(new File(baseDir, "consensus-module"))
                .ingressChannel("aeron:udp?term-length=64k")
                .logChannel(logControlChannel(nodeId, AeronServiceType.LOG_CONTROL))
                .archiveContext(aeronArchiveContext.clone())
                .deleteDirOnStart(deleteOnStart);

        // TODO provide
        final ExchangeCoreClusteredService service = new ExchangeCoreClusteredService();

        final ClusteredServiceContainer.Context serviceContainerContext = new ClusteredServiceContainer.Context();

        serviceContainerContext
                .aeronDirectoryName(aeronDir)
                .archiveContext(aeronArchiveContext.clone())
                .clusterDir(new File(baseDir, "service"))
                .clusteredService(service)
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
