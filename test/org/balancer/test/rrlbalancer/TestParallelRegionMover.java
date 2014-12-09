package org.balancer.test.rrlbalancer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.balancer.rrlbalancer.ParallelRegionMover;
import org.balancer.rrlbalancer.ParallelRegionMover.ParallelRegionMoverResult;
import org.balancer.rrlbalancer.RegionMoverInterface;
import org.junit.Assert;
import org.junit.Test;

public class TestParallelRegionMover {

    private final ExecutorService service = Executors.newFixedThreadPool(1);

    @Test
    public void testForCorrectFunctionality() throws IOException,
            InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executors = Executors.newFixedThreadPool(5);
        final int rpCount = 5;
        final ParallelRegionMover pRegionMover = new ParallelRegionMover(
                new RegionMoverInterface() {

                    @Override
                    public boolean move(RegionPlan rp) throws Exception {
                        return true;
                    }
                }, 5, executors);

        FutureTask<ParallelRegionMoverResult> futureTask = new FutureTask<ParallelRegionMoverResult>(
                new Callable<ParallelRegionMoverResult>() {

                    @Override
                    public ParallelRegionMoverResult call() throws Exception {
                        return pRegionMover
                                .moveRegionsParallel(prepareRegionPlans(rpCount));
                    }
                });
        service.execute(futureTask);

        ParallelRegionMoverResult result = futureTask.get(10000,
                TimeUnit.MILLISECONDS);
        Assert.assertEquals(result.noOfRegionsMovedSuc, rpCount);
        Assert.assertEquals(result.noOfRegionsMovedFail, 0);
        Assert.assertEquals(result.diffDestinations, 0);
        futureTask.cancel(true);
        executors.shutdownNow();
    }

    @Test
    public void testForCorrectFunctionalityWithDifferentDestinations()
            throws IOException, InterruptedException, ExecutionException,
            TimeoutException {
        ExecutorService executors = Executors.newFixedThreadPool(5);
        final int rpCount = 5;
        final ParallelRegionMover pRegionMover = new ParallelRegionMover(
                new RegionMoverInterface() {

                    @Override
                    public boolean move(RegionPlan rp) throws Exception {
                        return false;
                    }
                }, rpCount, executors);

        FutureTask<ParallelRegionMoverResult> futureTask = new FutureTask<ParallelRegionMoverResult>(
                new Callable<ParallelRegionMoverResult>() {

                    @Override
                    public ParallelRegionMoverResult call() throws Exception {
                        return pRegionMover
                                .moveRegionsParallel(prepareRegionPlans(rpCount));
                    }
                });
        service.execute(futureTask);

        ParallelRegionMoverResult result = futureTask.get(10000,
                TimeUnit.MILLISECONDS);
        Assert.assertEquals(result.noOfRegionsMovedSuc, rpCount);
        Assert.assertEquals(result.noOfRegionsMovedFail, 0);
        Assert.assertEquals(result.diffDestinations, rpCount);
        futureTask.cancel(true);
        executors.shutdownNow();
    }

    @Test
    public void testForCorrectFunctionalityWithFailures() throws IOException,
            InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executors = Executors.newFixedThreadPool(5);
        final int rpCount = 5;
        final ParallelRegionMover pRegionMover = new ParallelRegionMover(
                new RegionMoverInterface() {

                    @Override
                    public boolean move(RegionPlan rp) throws Exception {
                        throw new Exception("Exception occurred");
                    }
                }, rpCount, executors);

        FutureTask<ParallelRegionMoverResult> futureTask = new FutureTask<ParallelRegionMoverResult>(
                new Callable<ParallelRegionMoverResult>() {

                    @Override
                    public ParallelRegionMoverResult call() throws Exception {
                        return pRegionMover
                                .moveRegionsParallel(prepareRegionPlans(rpCount));
                    }
                });
        service.execute(futureTask);

        ParallelRegionMoverResult result = futureTask.get(10000,
                TimeUnit.MILLISECONDS);
        Assert.assertEquals(result.noOfRegionsMovedSuc, 0);
        Assert.assertEquals(result.noOfRegionsMovedFail, rpCount);
        Assert.assertEquals(result.diffDestinations, 0);
        futureTask.cancel(true);
        executors.shutdownNow();
    }

    @Test
    public void testForCorrectFunctionalityWithIncorrectLocks()
            throws IOException, InterruptedException, ExecutionException {
        ExecutorService executors = Executors.newFixedThreadPool(5);
        final int rpCount = 5;
        final ServerName snFirst = new ServerName("hostname", 1, 1);
        final ServerName snSecond = new ServerName("hostname", 2, 2);
        final ServerName snThird = new ServerName("hostname", 3, 3);
        final AtomicInteger count = new AtomicInteger(0);

        final ParallelRegionMover pRegionMover = new ParallelRegionMover(
                new RegionMoverInterface() {

                    @Override
                    public boolean move(RegionPlan rp) throws Exception {
                        count.incrementAndGet();
                        if (rp.getSource().equals(snFirst))
                            Thread.sleep(10000000);
                        return false;
                    }
                }, rpCount, executors);

        final List<RegionPlan> rps = new ArrayList<RegionPlan>();
        rps.add(new RegionPlan(new HRegionInfo(), snFirst, snSecond));
        rps.add(new RegionPlan(new HRegionInfo(), snFirst, snThird));

        FutureTask<ParallelRegionMoverResult> futureTask = new FutureTask<ParallelRegionMoverResult>(
                new Callable<ParallelRegionMoverResult>() {

                    @Override
                    public ParallelRegionMoverResult call() throws Exception {
                        return pRegionMover.moveRegionsParallel(rps);
                    }
                });
        service.execute(futureTask);

        try {
            futureTask.get(1000, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
            // lets ignore this exception
        }
        Assert.assertTrue(!futureTask.isDone());
        Assert.assertEquals(count.get(), 1);
        futureTask.cancel(true);
        executors.shutdownNow();
    }

    @Test
    public void testForCorrectFunctionalityWithSameStartAndEndLocks()
            throws IOException, InterruptedException, ExecutionException,
            TimeoutException {
        ExecutorService executors = Executors.newFixedThreadPool(5);
        final int rpCount = 5;
        final ServerName snFirst = new ServerName("hostname", 1, 1);
        final AtomicInteger count = new AtomicInteger(0);

        final ParallelRegionMover pRegionMover = new ParallelRegionMover(
                new RegionMoverInterface() {

                    @Override
                    public boolean move(RegionPlan rp) throws Exception {
                        count.incrementAndGet();
                        return true;
                    }
                }, rpCount, executors);

        final List<RegionPlan> rps = new ArrayList<RegionPlan>();
        rps.add(new RegionPlan(new HRegionInfo(), snFirst, snFirst));

        FutureTask<ParallelRegionMoverResult> futureTask = new FutureTask<ParallelRegionMoverResult>(
                new Callable<ParallelRegionMoverResult>() {

                    @Override
                    public ParallelRegionMoverResult call() throws Exception {
                        return pRegionMover.moveRegionsParallel(rps);
                    }
                });
        service.execute(futureTask);

        ParallelRegionMoverResult result = futureTask.get(10000000000l,
                TimeUnit.MILLISECONDS);
        Assert.assertEquals(result.noOfRegionsMovedSuc, 1);
        Assert.assertEquals(result.noOfRegionsMovedFail, 0);
        Assert.assertEquals(result.diffDestinations, 0);
        futureTask.cancel(true);
        executors.shutdownNow();
    }

    private static List<RegionPlan> prepareRegionPlans(int count) {
        List<RegionPlan> rps = new ArrayList<RegionPlan>();
        for (int i = 0; i < count; i++)
            rps.add(new RegionPlan(new HRegionInfo(), new ServerName("hostname"
                    + i, i, i), new ServerName("hostname" + (i + 1), (i + 1),
                    (i + 1))));
        return rps;
    }
}
