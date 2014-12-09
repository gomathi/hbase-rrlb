package org.balancer.rrlbalancer;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;

/**
 * Previously {@link #moveRegionsParallel(List)} was a fuction of {@link RelatedRegionsLoadBalancer}. But it became hard to do unit testing. This is a separate
 * rewrite of that function that can be unit testable.
 * 
 * @author gomes
 * 
 */
public class ParallelRegionMover {

    private static final Log LOG = LogFactory.getLog(ParallelRegionMover.class);
    private static final RegionPlan POISON_PILL = new RegionPlan(null, null,
            null);

    private final RegionMoverInterface regionMover;
    private final int noOfMoverThreads;
    private final ExecutorService executorService;

    public static final class ParallelRegionMoverResult {
        public final int noOfRegionsMovedSuc, noOfRegionsMovedFail,
                diffDestinations;

        public ParallelRegionMoverResult(
                int noOfRegionsMovedSuc, int noOfRegionsMovedFail,
                int diffDestinations) {
            this.noOfRegionsMovedSuc = noOfRegionsMovedSuc;
            this.noOfRegionsMovedFail = noOfRegionsMovedFail;
            this.diffDestinations = diffDestinations;
        }
    }

    public ParallelRegionMover(
            RegionMoverInterface moverUtil, int noOfMoverThreads,
            ExecutorService executorService) {
        this.regionMover = moverUtil;
        this.noOfMoverThreads = noOfMoverThreads;
        this.executorService = executorService;
    }

    public ParallelRegionMoverResult moveRegionsParallel(
            final List<RegionPlan> regionPlans) throws IOException,
            InterruptedException {
        long startTime = System.currentTimeMillis();
        if (regionPlans.size() == 0) {
            LOG.info("No regions to move. Skipping further tasks.");
            return new ParallelRegionMoverResult(0, 0, 0);
        }
        LOG.info("Region movement started.");

        final AtomicInteger noOfRegionsMovedSuc = new AtomicInteger(0);
        final AtomicInteger noOfRegionsMovedFail = new AtomicInteger(0);
        final AtomicInteger diffDestinations = new AtomicInteger(0);
        final ConcurrentHashMap<ServerName, Boolean> hostLocks = new ConcurrentHashMap<>();
        final BlockingQueue<RegionPlan> consumerQueue = new ArrayBlockingQueue<>(
                noOfMoverThreads);
        final ArrayDeque<RegionPlan> producerQueue = new ArrayDeque<>(
                regionPlans);
        final CountDownLatch stopLatch = new CountDownLatch(noOfMoverThreads);

        for (int i = 0; i < noOfMoverThreads; i++) {
            executorService.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        RegionPlan rp = null;
                        while (true) {
                            try {
                                rp = consumerQueue.take();
                                if (rp == POISON_PILL) {
                                    break;
                                }
                                if (!regionMover.move(rp))
                                    diffDestinations.incrementAndGet();
                                noOfRegionsMovedSuc.incrementAndGet();
                            }
                            catch (Exception e) {
                                LOG.error("Region move failed : " + rp, e);
                                noOfRegionsMovedFail.incrementAndGet();
                            }
                            finally {
                                if (rp != null && rp != POISON_PILL) {
                                    hostLocks.remove(rp.getDestination());
                                    hostLocks.remove(rp.getSource());
                                }
                            }
                        }
                    }
                    finally {
                        stopLatch.countDown();
                    }
                }
            });
        }

        Future<?> future = launchRegionMoverStatusUpdater(noOfRegionsMovedSuc,
                noOfRegionsMovedFail);

        while (!producerQueue.isEmpty()) {
            RegionPlan rp = producerQueue.removeFirst();
            Pair<ServerName, ServerName> sorted = sortServerNames(
                    rp.getSource(), rp.getDestination());
            boolean firstLockAcquired = false;
            boolean secondLockAcquired = false;
            boolean regionMoveScheduled = false;
            try {
                firstLockAcquired = hostLocks.putIfAbsent(sorted.getFirst(),
                        true) == null ? true : false;
                if (sorted.getFirst().equals(sorted.getSecond()))
                    secondLockAcquired = true;
                else
                    secondLockAcquired = hostLocks.putIfAbsent(
                            sorted.getSecond(), true) == null ? true : false;

                if (firstLockAcquired && secondLockAcquired) {
                    regionMoveScheduled = true;
                    consumerQueue.put(rp);
                }
                else {
                    producerQueue.addLast(rp);
                }
            }
            finally {
                if (!regionMoveScheduled) {
                    if (secondLockAcquired)
                        hostLocks.remove(sorted.getSecond());
                    if (firstLockAcquired)
                        hostLocks.remove(sorted.getFirst());
                }
            }
        }

        for (int i = 0; i < noOfMoverThreads; i++)
            consumerQueue.put(POISON_PILL);

        stopLatch.await();
        future.cancel(true);

        LOG.info("Total successful region movements : "
                + noOfRegionsMovedSuc.get() + "," + " and failed count: "
                + noOfRegionsMovedFail.get());
        LOG.info("No of regions that are placed into different hosts than the expected destination : "
                + diffDestinations.get());
        LOG.info("Region movement finished.");

        long endTime = System.currentTimeMillis();
        LOG.info("Total time took for moving regions (in ms): "
                + (endTime - startTime));

        return new ParallelRegionMoverResult(noOfRegionsMovedSuc.get(),
                noOfRegionsMovedFail.get(), diffDestinations.get());
    }

    private Future<?> launchRegionMoverStatusUpdater(
            final AtomicInteger noOfRegionsMovedSuc,
            final AtomicInteger noOfRegionsMovedFail) {
        return executorService.submit(new Runnable() {

            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(10000);
                        LOG.info("Region movement status updater - suc count: "
                                + noOfRegionsMovedSuc.get());
                        LOG.info("Region movement status updater - failed count: "
                                + noOfRegionsMovedFail.get());
                    }
                    catch (InterruptedException e) {
                        LOG.warn("Region movement status updater interrupted. Stop updating status further.");
                        break;
                    }

                }
            }
        });
    }

    private static Pair<ServerName, ServerName> sortServerNames(
            ServerName first, ServerName second) {
        if ((first.compareTo(second)) < 0) {
            return Pair.create(first, second);
        }
        else {
            return Pair.create(second, first);
        }
    }
}
