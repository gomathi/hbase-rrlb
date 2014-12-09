package org.balancer.rrlbalancer;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * If two regions share same startkey, endkey, they are called as region clusters.
 * 
 */
public class RegionClusterKey implements Comparable<RegionClusterKey> {
    public final byte[] startKey, endKey;
    public final String clusterName;

    public RegionClusterKey(
            String clusterName, byte[] startKey, byte[] endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.clusterName = clusterName;
    }

    @Override
    public int hashCode() {
        int prime = 31;
        int result = Arrays.hashCode(startKey);
        result = result * prime + Arrays.hashCode(endKey);
        result = result * prime + clusterName.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object anoObj) {
        if (anoObj == null || !(anoObj instanceof RegionClusterKey))
            return false;
        if (this == anoObj)
            return true;
        RegionClusterKey regionStartEndKeyObj = (RegionClusterKey) anoObj;
        if (Arrays.equals(startKey, regionStartEndKeyObj.startKey)
                && Arrays.equals(endKey, regionStartEndKeyObj.endKey)
                && clusterName.equals(regionStartEndKeyObj.clusterName))
            return true;
        return false;
    }

    @Override
    public int compareTo(RegionClusterKey o) {
        // TODO Auto-generated method stub
        int compRes = clusterName.compareTo(o.clusterName);
        if (compRes != 0)
            return compRes;
        compRes = Bytes.compareTo(startKey, o.startKey);
        if (compRes != 0)
            return compRes;
        compRes = Bytes.compareTo(endKey, o.endKey);
        return compRes;
    }
}
