package org.balancer.rrlbalancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Given a table name, gives the corresponding cluster name. The user has to initialize with the related tables information.
 * 
 */

@ThreadSafe
public class TableClusterMapping {

    private final static Random RANDOM = new Random(System.currentTimeMillis());
    private final static String CLUSTER_PREFIX = "cluster-";
    private final static String RANDOM_PREFIX = "random-";

    private final Map<Integer, String> clusterIdAndName = new HashMap<Integer, String>();
    private final List<Set<String>> clusters = new ArrayList<Set<String>>();

    public synchronized void addClusters(final List<Set<String>> clusters) {
        for (Set<String> cluster : clusters)
            addCluster(cluster);
    }

    public synchronized void addCluster(final Set<String> cluster) {
        Set<String> mCluster = new HashSet<String>(cluster);
        clusters.add(mCluster);
        String currClusterName = CLUSTER_PREFIX + clusters.size();
        clusterIdAndName.put(clusters.size(), currClusterName);
    }

    private int getClusterIndexOf(final String tableName) {
        for (int i = 0; i < clusters.size(); i++) {
            if (clusters.get(i).contains(tableName)) {
                return i + 1;
            }
        }
        return -1;
    }

    public boolean isPartOfAnyCluster(final String tableName) {
        return (getClusterIndexOf(tableName) != -1) ? true : false;
    }

    public String getClusterName(final String tableName) {
        int clusterIndex = getClusterIndexOf(tableName);
        if (clusterIndex != -1)
            return clusterIdAndName.get(clusterIndex);
        return RANDOM_PREFIX + RANDOM.nextInt();
    }
}