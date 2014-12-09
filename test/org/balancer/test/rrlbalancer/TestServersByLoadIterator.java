package org.balancer.test.rrlbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.balancer.rrlbalancer.ServerAndAllClusteredRegions;
import org.balancer.rrlbalancer.ServersByLoadIterator;
import org.junit.Assert;
import org.junit.Test;

public class TestServersByLoadIterator {

    private static final Random RANDOM = new Random(System.currentTimeMillis());

    @Test
    public void testForCorrectness() {
        int[] loads = { 1, 2, 3, 4 };

        List<ServerAndAllClusteredRegions> list = generateServerAndLoad(loads);
        ServersByLoadIterator itr = new ServersByLoadIterator(list.iterator(),
                3);
        testIterator(itr, 2);

        itr = new ServersByLoadIterator(list.iterator(), -1);
        testIterator(itr, 0);

        itr = new ServersByLoadIterator(list.iterator(), 5);
        testIterator(itr, 4);
    }

    private void testIterator(ServersByLoadIterator itr, int expectedCnt) {
        int testCnt = 0;
        while (itr.hasNext()) {
            itr.next();
            testCnt++;
        }

        Assert.assertEquals(expectedCnt, testCnt);
    }

    private List<ServerAndAllClusteredRegions> generateServerAndLoad(int[] loads) {
        List<ServerAndAllClusteredRegions> result = new ArrayList<ServerAndAllClusteredRegions>();
        for (int load : loads) {
            List<List<HRegionInfo>> clusteredRegions = new ArrayList<List<HRegionInfo>>();
            for (int i = 0; i < load; i++)
                clusteredRegions.add(new ArrayList<HRegionInfo>());
            ServerAndAllClusteredRegions sal = new ServerAndAllClusteredRegions(
                    new ServerName(Integer.toString(load), RANDOM.nextInt(),
                            RANDOM.nextInt()), clusteredRegions);
            result.add(sal);
        }
        return result;
    }
}
