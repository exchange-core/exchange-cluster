package exchange.core2.cluster.utils;

import net.openhft.affinity.AffinityLock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class AffinityThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(AffinityThreadFactory.class);

    // track all threads requested from the factory to avoid duplicate reservations.
    private final Set<Object> affinityReservations = new HashSet<>();

    private final ThreadAffinityMode threadAffinityMode;

    private static AtomicInteger globalThreadsCounter = new AtomicInteger();

    public AffinityThreadFactory(final ThreadAffinityMode threadAffinityMode) {
        this.threadAffinityMode = threadAffinityMode;
    }

    @Override
    public synchronized Thread newThread(@NotNull Runnable runnable) {

        // log.info("---- Requesting thread for {}", runnable);

        if (threadAffinityMode == ThreadAffinityMode.THREAD_AFFINITY_DISABLE) {
            return Executors.defaultThreadFactory().newThread(runnable);
        }

        if (affinityReservations.contains(runnable)) {
            log.warn("Task {} was pinned earlier", runnable);
//            return Executors.defaultThreadFactory().newThread(runnable);
        }

        affinityReservations.add(runnable);

        return new Thread(() -> executePinned(runnable));

    }

    private void executePinned(@NotNull Runnable runnable) {

        try (final AffinityLock lock = getAffinityLockSync()) {

            final int threadId = globalThreadsCounter.incrementAndGet();
            Thread.currentThread().setName(String.format("Thread-AF-%d-cpu%d", threadId, lock.cpuId()));

            log.debug("{} will be running on thread={} pinned to cpu {}",
                    runnable, Thread.currentThread().getName(), lock.cpuId());

            runnable.run();

        } finally {
            log.debug("Removing cpu lock/reservation from {}", runnable);
            synchronized (this) {
                affinityReservations.remove(runnable);
            }
        }
    }

    private synchronized AffinityLock getAffinityLockSync() {
        return threadAffinityMode == ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE
                ? AffinityLock.acquireCore()
                : AffinityLock.acquireLock();
    }

    public enum ThreadAffinityMode {
        THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE,
        THREAD_AFFINITY_ENABLE_PER_LOGICAL_CORE,
        THREAD_AFFINITY_DISABLE
    }

}
