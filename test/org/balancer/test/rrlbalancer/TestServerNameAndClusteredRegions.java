package org.balancer.test.rrlbalancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.balancer.rrlbalancer.RegionClusterKey;
import org.balancer.rrlbalancer.ServerNameAndClusteredRegions;
import org.junit.Assert;
import org.junit.Test;

public class TestServerNameAndClusteredRegions {
    private final static Random RANDOM = new Random(System.currentTimeMillis());

    @Test
    public void testForCorrectness() {
        RegionClusterKey rcKey = new RegionClusterKey("d", Bytes.toBytes("d"),
                Bytes.toBytes("f"));
        ServerNameAndClusteredRegions snacrFirst = new ServerNameAndClusteredRegions(
                new ServerName("localhost", RANDOM.nextInt(), RANDOM.nextLong()),
                rcKey, generateHRegionsList(3));

        ServerNameAndClusteredRegions snacrSecond = new ServerNameAndClusteredRegions(
                new ServerName("localhost", RANDOM.nextInt(), RANDOM.nextLong()),
                rcKey, generateHRegionsList(4));

        List<ServerNameAndClusteredRegions> testSet = new ArrayList<ServerNameAndClusteredRegions>();
        testSet.add(snacrSecond);
        testSet.add(snacrFirst);

        Collections
                .sort(testSet,
                        new ServerNameAndClusteredRegions.ServerNameAndClusteredRegionsComparator());

        Assert.assertEquals(testSet.get(0), snacrFirst);
        Assert.assertEquals(testSet.get(1), snacrSecond);

        testSet.clear();

        RegionClusterKey rcKeyAno = new RegionClusterKey("a",
                Bytes.toBytes("a"), Bytes.toBytes("c"));

        ServerNameAndClusteredRegions snacrThird = new ServerNameAndClusteredRegions(
                new ServerName("localhost", RANDOM.nextInt(), RANDOM.nextLong()),
                rcKeyAno, generateHRegionsList(4));

        testSet.add(snacrSecond);
        testSet.add(snacrFirst);
        testSet.add(snacrThird);

        Collections
                .sort(testSet,
                        new ServerNameAndClusteredRegions.ServerNameAndClusteredRegionsComparator());

        Assert.assertEquals(testSet.get(0), snacrThird);
        Assert.assertEquals(testSet.get(1), snacrFirst);
        Assert.assertEquals(testSet.get(2), snacrSecond);
    }

    private List<HRegionInfo> generateHRegionsList(int count) {
        List<HRegionInfo> hRegions = new ArrayList<HRegionInfo>();
        for (int i = 0; i < count; i++)
            hRegions.add(new HRegionInfo());
        return hRegions;
    }
}
