package org.balancer.rrlbalancer;

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

/**
 * Data structure that holds servername and 'load'.
 */
public class ServerAndAllClusteredRegions {
    private final ServerName sn;
    private final List<List<HRegionInfo>> clusteredRegions;

    public ServerAndAllClusteredRegions(
            final ServerName sn, final List<List<HRegionInfo>> clusteredRegions) {
        this.sn = sn;
        this.clusteredRegions = clusteredRegions;
    }

    public ServerName getServerName() {
        return this.sn;
    }

    public int getLoad() {
        return this.clusteredRegions.size();
    }

    public List<HRegionInfo> removeNextCluster() {
        if (clusteredRegions.size() > 0)
            return clusteredRegions.remove(0);
        throw new NoSuchElementException();
    }

    public void addCluster(List<HRegionInfo> cluster) {
        clusteredRegions.add(cluster);
    }

    public static class ServerAndLoadComparator implements
            Comparator<ServerAndAllClusteredRegions> {

        @Override
        public int compare(ServerAndAllClusteredRegions salFirst,
                ServerAndAllClusteredRegions salSecond) {
            int diff = salFirst.getLoad() - salSecond.getLoad();
            return diff != 0 ? diff : salFirst.getServerName().compareTo(
                    salSecond.getServerName());
        }

    }
}