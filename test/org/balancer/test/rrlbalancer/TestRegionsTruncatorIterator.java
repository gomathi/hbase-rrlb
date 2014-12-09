package org.balancer.test.rrlbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.balancer.rrlbalancer.OverloadedRegionsRemover;
import org.balancer.rrlbalancer.ServerAndAllClusteredRegions;
import org.junit.Assert;
import org.junit.Test;

public class TestRegionsTruncatorIterator {

    private static final Random RANDOM = new Random(System.currentTimeMillis());

    @Test
    public void testForCorrectness() {
        int[] loads = { 1, 2 };

        List<ServerAndAllClusteredRegions> list = generateServerAndLoad(loads);
        OverloadedRegionsRemover tItr = new OverloadedRegionsRemover(
                list.iterator(), 5);
        testIterator(tItr, 0);

        list = generateServerAndLoad(loads);
        tItr = new OverloadedRegionsRemover(list.iterator(), 1);
        testIterator(tItr, 1);

        list = generateServerAndLoad(loads);
        tItr = new OverloadedRegionsRemover(list.iterator(), 0);
        testIterator(tItr, 3);
    }

    private void testIterator(OverloadedRegionsRemover itr, int expectedCnt) {
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
            for (int i = 0; i < load; i++) {
                List<HRegionInfo> list = new ArrayList<HRegionInfo>();
                list.add(new HRegionInfo());
                clusteredRegions.add(list);
            }
            ServerAndAllClusteredRegions sal = new ServerAndAllClusteredRegions(
                    new ServerName(Integer.toString(load), RANDOM.nextInt(),
                            RANDOM.nextInt()), clusteredRegions);
            result.add(sal);
        }
        return result;
    }

}
