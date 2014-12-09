package org.balancer.rrlbalancer;

import static org.balancer.rrlbalancer.Utils.clearValues;
import static org.balancer.rrlbalancer.Utils.cluster;
import static org.balancer.rrlbalancer.Utils.getMapEntriesForKeys;
import static org.balancer.rrlbalancer.Utils.getValues;
import static org.balancer.rrlbalancer.Utils.getValuesAsList;
import static org.balancer.rrlbalancer.Utils.intersect;
import static org.balancer.rrlbalancer.Utils.minus;
import static org.balancer.rrlbalancer.Utils.reverseMap;
import static org.balancer.rrlbalancer.Utils.tryParse;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.balancer.rrlbalancer.OverloadedRegionsRemover.TruncatedElement;
import org.balancer.rrlbalancer.Utils.ClusterDataKeyGenerator;

import com.google.common.base.Joiner;
import com.google.common.collect.ListMultimap;

/**
 * 
 * A load balancer which makes sure related regions are placed on the same Region server. Without this, related regions might be placed on different Region
 * servers, and will cause increased latency during the processing.
 * 
 * For this class to do its functionality, it needs to be provided with related tables information. Look at {@link #RelatedRegionsLoadBalancer(List)}.
 * 
 * NOTE: By default Hbase uses 'hbase.master.loadbalance.bytable' value to be true. With this configuration, related tables' regions will not be passed to
 * {@link #balanceCluster(Map)} method. It is necessary that you set this configuration to be false.
 */

public class RelatedRegionsLoadBalancer implements LoadBalancer {
    private static final Log LOG = LogFactory
            .getLog(RelatedRegionsLoadBalancer.class);
    private static final String HOST_WEIGHT_UPD_LISTENER_THREAD_NAME = "RRLBalancer-HostWeightsFileListener";
    private static final String REGION_MOVER_THREAD_NAME = "RRLBalancer-RegionMoverThread";
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final long MAX_BAL_RUNNING_TIME = 2 * 60 * 60 * 1000;
    private final static long MAX_TIME_TO_WAIT_REGION_MOVE = 5 * 60 * 1000; // In milliseconds

    private final boolean balanceInBG;
    private final AtomicBoolean balancerRunning = new AtomicBoolean(false);
    private final AtomicLong balancerStartedTime = new AtomicLong(0);
    private final TableClusterMapping tableToClusterMapObj = new TableClusterMapping();
    private final Map<String, Integer> hostNamesAndWeight = new HashMap<String, Integer>();
    private final Object hostNameAndWeightLock = new Object();

    /**
     * Used by {@link #randomAssignment(HRegionInfo, List)} method. {@link #randomAssignment(HRegionInfo, List)} is called during RS failure. Using this map,
     * related regions are placed on the same RS.
     */
    private final ConcurrentHashMap<RegionClusterKey, ServerName> regionsAndServers = new ConcurrentHashMap<RegionClusterKey, ServerName>();
    private volatile float slop;
    private volatile Configuration conf;
    private volatile HostWeightsFileChangeListener fileChangeListener;
    private volatile Thread fileChangeListenerThread;
    private volatile int noOfMoverThreads;
    private volatile int totThreads;
    private volatile long maxTimeToWaitForRegionMove;
    private volatile ExecutorService executorService;

    private final ClusterDataKeyGenerator<HRegionInfo, RegionClusterKey> regionKeyGener = new ClusterDataKeyGenerator<HRegionInfo, RegionClusterKey>() {

        @Override
        public RegionClusterKey generateKey(HRegionInfo hregionInfo) {
            return new RegionClusterKey(
                    tableToClusterMapObj.getClusterName(hregionInfo
                            .getTableNameAsString()),
                    hregionInfo.getStartKey(), hregionInfo.getEndKey());
        }

    };

    private final ClusterDataKeyGenerator<ServerName, String> hostKeyGener = new ClusterDataKeyGenerator<ServerName, String>() {

        @Override
        public String generateKey(ServerName sn) {
            return sn.getHostname();
        }

    };

    private class HostWeightsFileChangeListener implements Runnable {

        private final String filePath;
        private final String fileName;
        private final Path dirPath;
        private volatile boolean stopRequested;

        public HostWeightsFileChangeListener(
                String dirName, String fileName) {
            this.dirPath = Paths.get(dirName);
            this.fileName = fileName;
            this.filePath = dirName + "/" + fileName;
        }

        public void requestStop() {
            this.stopRequested = true;
        }

        public void updateHostsWeight() {
            final Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(filePath));
            }
            catch (IOException e) {
                LOG.warn("IO Exception occurred while reading " + filePath, e);
                return;
            }
            Map<String, Integer> input = new TreeMap<String, Integer>();
            for (String key : properties.stringPropertyNames()) {
                String value = properties.getProperty(key);
                Integer weight = tryParse(value);
                if (weight == null)
                    LOG.warn("Invalid weight is provided : (" + key + ","
                            + value + "). Skipping the host weight.");
                else
                    input.put(key, weight);
            }
            updateHostNameAndWeight(input);
        }

        public void run() {
            LOG.info("HostWeightsFileChangeListener - Starting");
            if (dirPath != null) {
                try {
                    final WatchService watchService = FileSystems.getDefault()
                            .newWatchService();
                    dirPath.register(watchService,
                            StandardWatchEventKinds.ENTRY_CREATE,
                            StandardWatchEventKinds.ENTRY_MODIFY);
                    while (true) {
                        final WatchKey wk = watchService.take();
                        for (final WatchEvent<?> event : wk.pollEvents()) {
                            @SuppressWarnings("unchecked")
                            final Path eventPath = ((WatchEvent<Path>) event)
                                    .context();
                            if (eventPath.getFileName().toString()
                                    .contains(fileName)) {
                                LOG.info(fileName
                                        + " has been changed.Updating hosts weight");
                                updateHostsWeight();
                                boolean valid = wk.reset();
                                if (!valid) {
                                    LOG.warn("Key has been unregistered. Not watching the file changes anymore.");
                                    break;
                                }
                            }
                        }
                    }
                }
                catch (IOException | InterruptedException e) {
                    if (!stopRequested)
                        LOG.warn(
                                "Exception occurred in watch service. Not watching host weight conf file anymore.",
                                e);
                    return;
                }
            }
            else {
                LOG.warn("Given directory path is not valid. Not watching the host weights conf file.");
            }
        }
    }

    public RelatedRegionsLoadBalancer(
            List<Set<String>> clusteredTableNamesList, boolean balanceInBg) {
        this.balanceInBG = balanceInBg;
        if (clusteredTableNamesList != null) {
            LOG.info("Adding " + clusteredTableNamesList
                    + " to cluster mapping.");
            tableToClusterMapObj.addClusters(clusteredTableNamesList);
        }
    }

    public RelatedRegionsLoadBalancer(
            List<Set<String>> clusteredTableNamesList) {
        this(clusteredTableNamesList, true);
    }

    public RelatedRegionsLoadBalancer(
            List<Set<String>> clusteredTableNamesList,
            String hostWeightConfigDir, String hostWeightConfigFile,
            boolean balanceInBG) {
        this(clusteredTableNamesList, balanceInBG);
        launchThreadToUpdateHostsWeightInBg(hostWeightConfigDir,
                hostWeightConfigFile);
    }

    public RelatedRegionsLoadBalancer(
            List<Set<String>> clusteredTableNamesList,
            String hostWeightConfigDir, String hostWeightConfigFile) {
        this(clusteredTableNamesList, hostWeightConfigDir,
                hostWeightConfigFile, true);
    }

    public RelatedRegionsLoadBalancer(
            List<Set<String>> clusteredTableNamesList,
            Map<String, Integer> hostNameAndWeight, boolean balanceInBG) {
        this(clusteredTableNamesList, balanceInBG);
        updateHostNameAndWeight(hostNameAndWeight);
    }

    private void launchThreadToUpdateHostsWeightInBg(String confFileDir,
            String confFileName) {
        fileChangeListener = new HostWeightsFileChangeListener(confFileDir,
                confFileName);
        fileChangeListener.updateHostsWeight();
        fileChangeListenerThread = new Thread(fileChangeListener,
                HOST_WEIGHT_UPD_LISTENER_THREAD_NAME);
        fileChangeListenerThread.setDaemon(true);
        fileChangeListenerThread.start();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.slop = conf.getFloat("hbase.regions.slop", (float) 0.2);
        if (slop < 0)
            slop = 0;
        else if (slop > 1)
            slop = 1;

        this.noOfMoverThreads = conf
                .getInt("rrlbalancer.moverthreadscount", 10);
        this.totThreads = noOfMoverThreads + 2;
        this.maxTimeToWaitForRegionMove = conf.getLong(
                "rrlbalancer.maxtimeforregionmove",
                MAX_TIME_TO_WAIT_REGION_MOVE);

        executorService = Executors.newFixedThreadPool(totThreads,
                new ThreadFactory() {

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, REGION_MOVER_THREAD_NAME);
                        t.setDaemon(true);
                        return t;
                    }
                });
        LOG.info("Slop value : " + slop);
        LOG.info("No of mover threads : " + noOfMoverThreads);
    }

    @Override
    public void setClusterStatus(ClusterStatus st) {
    }

    @Override
    public void setMasterServices(MasterServices masterServices) {
    }

    @Override
    public List<RegionPlan> balanceCluster(
            Map<ServerName, List<HRegionInfo>> clusterState) {
        if (clusterState == null || clusterState.size() == 0) {
            LOG.debug("Empty cluster has been passed. Not balancing the cluster.");
            return null;
        }

        long startTime = System.currentTimeMillis();

        LOG.info("Balance cluster triggered: ");

        // We dont want to move around meta regions between region servers.
        for (ServerName serverName : clusterState.keySet()) {
            List<HRegionInfo> regions = clusterState.get(serverName);
            List<HRegionInfo> nonMetaRegions = removeMetaRegions(regions);
            clusterState.put(serverName, nonMetaRegions);
        }

        // First, lets move all the related regions to one region server, if the
        // related regions are fragmented across region servers.
        Map<HRegionInfo, RegionPlan> movRelRegionsResult = defragmentRelatedRegions(clusterState);

        // Adds virtual servers to the cluster state if some physical hosts are
        // more capable of other hosts.
        Map<ServerName, ServerName> virtServToPhyServ = doWeightedBalancing(clusterState);

        // Lets balance related regions size across all the region servers.
        Map<HRegionInfo, RegionPlan> balanceRelRegionsResult = balanceRelatedRegions(clusterState);

        // Remove virtual servers, and prepare region plans with original
        // physical
        // servers.
        Map<HRegionInfo, RegionPlan> rpsWithOriRS = removeVirtualServers(
                balanceRelRegionsResult, virtServToPhyServ);

        // Lets combine the result, and prepare a final region plans.
        final List<RegionPlan> result = getValues(merge(movRelRegionsResult,
                rpsWithOriRS));

        long endTime = System.currentTimeMillis();

        LOG.info("Total no of region plans prepared :" + result.size());
        LOG.info("Balance cluster finished. Total time took for preparing region plans (in ms): "
                + (endTime - startTime));
        if (balanceInBG) {
            if (balancerRunning.compareAndSet(false, true)) {
                balancerStartedTime.set(System.currentTimeMillis());
                executorService.submit(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            moveRegionsParallel(result);
                        }
                        catch (IOException | InterruptedException e) {
                            LOG.warn(
                                    "Exception occurred while moving regions.",
                                    e);
                        }
                        finally {
                            balancerRunning.set(false);
                        }
                    }
                });
            }
            else {
                LOG.info("Previous region movements are still happening. Skipping current balancer.");
                // This should not happen, but lets log for exceptional cases.
                long prevBalancerStartedTime = balancerStartedTime.get();
                long timeTookForBalancing = System.currentTimeMillis()
                        - prevBalancerStartedTime;
                if (prevBalancerStartedTime > 0
                        && timeTookForBalancing > MAX_BAL_RUNNING_TIME) {
                    LOG.warn("Balancer is running for more than "
                            + (timeTookForBalancing));
                }
            }
            return null;
        }
        else
            return result;
    }

    public void updateHostNameAndWeight(
            Map<String, Integer> newHostNameAndWeight) {
        synchronized (hostNameAndWeightLock) {
            LOG.info("Updating hostNamesAndWeight internal map "
                    + newHostNameAndWeight.size() + " entries.");
            LOG.info(newHostNameAndWeight);
            hostNamesAndWeight.clear();
            hostNamesAndWeight.putAll(newHostNameAndWeight);
        }
    }

    /**
     * Reads each region plan, and replaces the virtual server names of source and destination by actual physical server name.
     * 
     * @param regionInfoAndPlan
     * @param virtServToPhyServ
     * @return
     */
    private Map<HRegionInfo, RegionPlan> removeVirtualServers(
            Map<HRegionInfo, RegionPlan> regionInfoAndPlan,
            Map<ServerName, ServerName> virtServToPhyServ) {
        Map<HRegionInfo, RegionPlan> result = new HashMap<HRegionInfo, RegionPlan>();
        for (Map.Entry<HRegionInfo, RegionPlan> rp : regionInfoAndPlan
                .entrySet()) {
            ServerName src = rp.getValue().getSource();
            ServerName dest = rp.getValue().getDestination();
            RegionPlan nrp;

            if (virtServToPhyServ.containsKey(src)) {
                nrp = new RegionPlan(
                        rp.getKey(),
                        virtServToPhyServ.get(src),
                        virtServToPhyServ.containsKey(dest) ? (virtServToPhyServ
                                .get(dest)) : dest);
            }
            else if (virtServToPhyServ.containsKey(dest)) {
                nrp = new RegionPlan(rp.getKey(), src,
                        virtServToPhyServ.get(dest));
            }
            else
                nrp = new RegionPlan(rp.getKey(), src, dest);
            if (!nrp.getSource().equals(nrp.getDestination()))
                result.put(rp.getKey(), nrp);
        }
        return result;
    }

    /**
     * Adds virtual servers as per the given weight of each server, and tries to assign related regions to each virtual server from actual physical server. Size
     * of related regions that is assigned to each server in the final result is in between >= min and <= max. Where min and max is calcualted from the average
     * no of regions.
     * 
     * This function changes the clusterState such that each physical server will try to retain the previously assigned regions.
     * 
     * @param clusterState
     * @return
     */
    private Map<ServerName, ServerName> doWeightedBalancing(
            Map<ServerName, List<HRegionInfo>> clusterState) {
        Map<ServerName, ServerName> virtServToPhyServ = addVirtualServers(clusterState
                .keySet());
        Map<ServerName, List<ServerName>> phyServerToVirtServer = new HashMap<>();
        for (Map.Entry<ServerName, ServerName> entry : virtServToPhyServ
                .entrySet()) {
            if (!phyServerToVirtServer.containsKey(entry.getValue())) {
                phyServerToVirtServer.put(entry.getValue(),
                        new ArrayList<ServerName>());
            }
            phyServerToVirtServer.get(entry.getValue()).add(entry.getKey());
        }

        int totServers = clusterState.keySet().size()
                + virtServToPhyServ.keySet().size();
        int totClusteredRegions = 0;

        Map<ServerName, List<List<HRegionInfo>>> serverAndClusteredRegions = new HashMap<>();

        for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState
                .entrySet()) {
            List<List<HRegionInfo>> clusteredRegions = getValuesAsList(clusterRegions(entry
                    .getValue()));
            totClusteredRegions += clusteredRegions.size();
            serverAndClusteredRegions.put(entry.getKey(), clusteredRegions);
        }

        int avg = totClusteredRegions / totServers;
        int min = (int) avg;
        int max = (totClusteredRegions % totServers == 0) ? min : (min + 1);

        for (Map.Entry<ServerName, List<ServerName>> entry : phyServerToVirtServer
                .entrySet()) {
            clusterState.remove(entry.getKey());
            List<List<HRegionInfo>> clusteredRegions = serverAndClusteredRegions
                    .get(entry.getKey());
            Map<ServerName, Integer> snAndCount = new HashMap<>();
            int currIndex = 0;
            for (ServerName server : entry.getValue()) {
                snAndCount.put(server, 0);
                List<HRegionInfo> regions = new ArrayList<>();
                int nextIndex = Math.min(currIndex + min,
                        clusteredRegions.size());
                for (int i = currIndex; i < nextIndex; i++) {
                    regions.addAll(clusteredRegions.get(i));
                    snAndCount.put(server, snAndCount.get(server) + 1);
                }
                currIndex = nextIndex;
                clusterState.put(server, regions);
            }
            int nextIndex = clusteredRegions.size() - max;
            Iterator<ServerName> itr = entry.getValue().iterator();
            ServerName sn = null;
            for (; currIndex < nextIndex; currIndex++) {
                if (sn == null || snAndCount.get(sn) >= max) {
                    while (itr.hasNext()) {
                        sn = itr.next();
                        if (clusterState.containsKey(sn)
                                && snAndCount.get(sn) < max) {
                            break;
                        }
                    }
                }
                if (sn == null || snAndCount.get(sn) >= max) {
                    break;
                }
                clusterState.get(sn).addAll(clusteredRegions.get(currIndex));
                snAndCount.put(sn, snAndCount.get(sn) + 1);
            }
            List<HRegionInfo> regions = new ArrayList<>();
            for (int i = currIndex; i < clusteredRegions.size(); i++)
                regions.addAll(clusteredRegions.get(i));
            clusterState.put(entry.getKey(), regions);
        }

        return virtServToPhyServ;
    }

    private Map<ServerName, ServerName> addVirtualServers(
            Set<ServerName> phyServers) {
        // TODO Auto-generated method stub
        Map<String, List<ServerName>> clusteredServers = clusterServers(phyServers);
        Map<ServerName, ServerName> virtServToPhyServ = new HashMap<ServerName, ServerName>();
        synchronized (hostNameAndWeightLock) {
            for (Map.Entry<String, List<ServerName>> entry : clusteredServers
                    .entrySet()) {
                if (hostNamesAndWeight.containsKey(entry.getKey())) {
                    int weight = hostNamesAndWeight.get(entry.getKey()) - 1;
                    int noOfVirtsPerRS = (weight % entry.getValue().size()) == 0 ? (weight / entry
                            .getValue().size()) : ((weight / entry.getValue()
                            .size()) + 1);
                    Iterator<ServerName> serverItr = entry.getValue()
                            .iterator();
                    while (weight > 0) {
                        int min = Math.min(weight, noOfVirtsPerRS);
                        ServerName phyServer = serverItr.next();
                        for (int i = 1; i <= min; i++) {
                            ServerName virtServer = null;
                            while (true) {
                                virtServer = randomServer();
                                if (!phyServers.contains(virtServer)
                                        && !virtServToPhyServ
                                                .containsKey(virtServer))
                                    break;
                            }
                            virtServToPhyServ.put(virtServer, phyServer);
                        }
                        weight -= min;
                    }
                }
            }
        }
        return virtServToPhyServ;
    }

    private ServerName randomServer() {
        // TODO Auto-generated method stub
        String serverName = "server" + RANDOM.nextInt();
        ServerName randomServer = new ServerName(serverName,
                RANDOM.nextInt(60000), RANDOM.nextLong());
        return randomServer;
    }

    /**
     * First step of {@link #balanceCluster(Map)}. If related regions are shared between two or more region servers, those related regions will be moved to one
     * region server.
     * 
     * 
     * Algorithm works as following
     * 
     * <ol>
     * 
     * <li>Input
     * <li>Servers are S1, S2. Tables are T1, T2.
     * <li>T1 contains regions [A,B]. T2 contains regions [X,Y].
     * <li>A & X's key range -> [1..3], B & Y's key range -> [4..6].
     * <li>S1 hosts [A,B] and S2 hosts [X,Y]
     * 
     * <li>Expected Output
     * <li>(S1 or S2) should only host [A,X] and, (S1 or S2) should host [B,Y]
     * 
     * <li>Algorithm
     * <li>Cluster all regions of each server, and put the clusters into a priority queue. In the priority queue, the clusters are sorted by
     * {@link RegionClusterKey} and cluster size. Hence, if two region servers have parts of related regions, the parts will be returned as combined elements.
     * 
     * <li>Prepare the region plan for the region movements.
     * 
     * <ol>
     * 
     * 
     * @param clusterState
     * @return
     */
    private Map<HRegionInfo, RegionPlan> defragmentRelatedRegions(
            Map<ServerName, List<HRegionInfo>> clusterState) {
        Map<HRegionInfo, RegionPlan> result = new HashMap<HRegionInfo, RegionPlan>();

        long startTime = System.currentTimeMillis();
        LOG.info("Defragmentation triggered : ");

        int totDefragRegions = 0;

        List<ServerNameAndClusteredRegions> snacrList = new ArrayList<ServerNameAndClusteredRegions>();
        for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState
                .entrySet()) {
            ServerName serverName = entry.getKey();
            Map<RegionClusterKey, List<HRegionInfo>> clusteredRegions = clusterRegions(entry
                    .getValue());
            for (Map.Entry<RegionClusterKey, List<HRegionInfo>> innerEntry : clusteredRegions
                    .entrySet()) {
                snacrList.add(new ServerNameAndClusteredRegions(serverName,
                        innerEntry.getKey(), innerEntry.getValue()));
            }
        }

        clearValues(clusterState);
        Collections
                .sort(snacrList,
                        new ServerNameAndClusteredRegions.ServerNameAndClusteredRegionsComparator());

        int beg = 0;
        for (int prev = 0, curr = 1; curr < snacrList.size(); curr++) {
            prev = curr - 1;
            if (!snacrList.get(prev).getRegionClusterKey()
                    .equals(snacrList.get(curr).getRegionClusterKey())) {
                totDefragRegions += defragmentRelatedRegionsHelper(beg, prev,
                        result, snacrList, clusterState);
                beg = curr;
            }
        }
        if (snacrList.size() > 0)
            totDefragRegions += defragmentRelatedRegionsHelper(beg,
                    snacrList.size() - 1, result, snacrList, clusterState);
        long endTime = System.currentTimeMillis();
        LOG.info("Total no of defragmented related regions : "
                + totDefragRegions);
        LOG.info("Total time took for defragmentation (in ms):"
                + (endTime - startTime));
        return result;
    }

    private int defragmentRelatedRegionsHelper(int beg, int last,
            Map<HRegionInfo, RegionPlan> result,
            List<ServerNameAndClusteredRegions> snacrList,
            Map<ServerName, List<HRegionInfo>> clusterState) {
        int cntDefragRegions = 0;
        ServerNameAndClusteredRegions dest = snacrList.get(last);
        if (last - beg > 0) {
            for (int temp = beg; temp < last; temp++) {
                ServerNameAndClusteredRegions src = snacrList.get(temp);
                cntDefragRegions += src.getClusteredRegions().size();
                for (HRegionInfo hri : src.getClusteredRegions()) {
                    result.put(hri, new RegionPlan(hri, src.getServerName(),
                            dest.getServerName()));
                    clusterState.get(dest.getServerName()).add(hri);
                }
            }
        }
        clusterState.get(dest.getServerName()).addAll(
                dest.getClusteredRegions());
        return cntDefragRegions;
    }

    /**
     * Balances load across all the region servers.
     * 
     * Calculates the min and max of regions that could be handled by a region server.
     * 
     * If the value of regionssize / noofregionservers is an integer, then all servers will have same no of regions. Otherwise all region servers will differ by
     * regions size at most 1.
     * 
     * 
     * @param clusterState
     * @return
     */
    private Map<HRegionInfo, RegionPlan> balanceRelatedRegions(
            Map<ServerName, List<HRegionInfo>> clusterState) {

        int maxRegionsCluster = 0;
        int numServers = clusterState.size();
        int numRegionsCluster = 0;

        long startTime = System.currentTimeMillis();

        List<ServerAndAllClusteredRegions> serversByLoad = new ArrayList<ServerAndAllClusteredRegions>();
        for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState
                .entrySet()) {
            List<List<HRegionInfo>> clusteredServerRegions = getValuesAsList(clusterRegions(entry
                    .getValue()));
            numRegionsCluster += clusteredServerRegions.size();
            serversByLoad.add(new ServerAndAllClusteredRegions(entry.getKey(),
                    clusteredServerRegions));

            if (maxRegionsCluster < clusteredServerRegions.size())
                maxRegionsCluster = clusteredServerRegions.size();
        }

        Collections.sort(serversByLoad,
                new ServerAndAllClusteredRegions.ServerAndLoadComparator());

        float avg = (float) numRegionsCluster / numServers;
        int floor = (int) Math.floor(avg * (1 - slop));
        int ceiling = (int) Math.ceil(avg * (1 + slop));

        int first = 0;
        int last = serversByLoad.size() - 1;

        LOG.info("Cluster details before balancing: " + "servers=" + numServers
                + " " + "related-regions-set-size=" + numRegionsCluster
                + " average=" + avg + " " + "mostloaded="
                + serversByLoad.get(last).getLoad() + " leastloaded="
                + serversByLoad.get(first).getLoad());
        if ((serversByLoad.get(last).getLoad() <= ceiling && serversByLoad.get(
                first).getLoad() >= floor)
                || maxRegionsCluster == 1) {
            // Skipped because no server outside (min,max) range
            LOG.info("Cluster is balanced as per related regions set distribution across region servers.. Skipping further operations.");
            return new HashMap<HRegionInfo, RegionPlan>();
        }

        int min = (int) avg;
        int max = (numRegionsCluster % numServers) == 0 ? min : min + 1;

        Map<HRegionInfo, RegionPlan> fPartial = truncateRegionServersToMaxRegions(
                serversByLoad, min, max);
        Map<HRegionInfo, RegionPlan> sPartial = balanceRegionServersToMinRegions(
                serversByLoad, min);

        long endTime = System.currentTimeMillis();

        LOG.info("Cluster details after balancing: " + "servers=" + numServers
                + " " + "related-regions-set-size=" + numRegionsCluster
                + " average=" + avg + " " + "mostloaded="
                + serversByLoad.get(last).getLoad() + " leastloaded="
                + serversByLoad.get(first).getLoad());

        LOG.info("Total time took for balanceRelatedRegions (in ms):"
                + (endTime - startTime));

        return merge(fPartial, sPartial);
    }

    /**
     * A region can be moved from through many servers. For example, s1 -> s2 -> s3. So the final region plan should be s1 -> s3.
     * 
     * This function combines the two partial results and prepares the merged result.
     * 
     * @param fPartial
     * @param sPartial
     * @return
     */
    private Map<HRegionInfo, RegionPlan> merge(
            Map<HRegionInfo, RegionPlan> fPartial,
            Map<HRegionInfo, RegionPlan> sPartial) {
        for (Map.Entry<HRegionInfo, RegionPlan> entry : fPartial.entrySet()) {
            HRegionInfo hri = entry.getKey();
            RegionPlan rp = entry.getValue();
            if (sPartial.containsKey(hri)) {
                RegionPlan combinedRegionPlan = new RegionPlan(hri,
                        rp.getSource(), sPartial.get(hri).getDestination());
                sPartial.put(hri, combinedRegionPlan);
            }
            else
                sPartial.put(hri, rp);
            RegionPlan rpResult = sPartial.get(hri);
            if (rpResult.getSource().equals(rpResult.getDestination()))
                sPartial.remove(hri);
        }
        return sPartial;
    }

    private List<HRegionInfo> removeMetaRegions(List<HRegionInfo> regions) {
        List<HRegionInfo> result = new ArrayList<HRegionInfo>();
        for (HRegionInfo region : regions) {
            if (!region.isMetaRegion() && !region.isRootRegion())
                result.add(region);
        }
        return result;
    }

    /**
     * Makes sure no region servers are overloaded beyond 'max' regions.
     * 
     * This is done by removing regions from all region servers which have size > max, and putting them on region servers that have < max regions. At the end
     * all region servers will have size <= max.
     * 
     * This does not guarantee all servers have >= min regions though. Look at {@link #balanceRegionServersToMinRegions(NavigableSet, int)} for bringing all
     * region servers to a limit of >= min.
     * 
     * @param serversByLoad
     * @param min
     * @param max
     * @return
     */
    private Map<HRegionInfo, RegionPlan> truncateRegionServersToMaxRegions(
            List<ServerAndAllClusteredRegions> serversByLoad, int min, int max) {
        Map<HRegionInfo, RegionPlan> result = new HashMap<HRegionInfo, RegionPlan>();

        LOG.info("truncateRegionServersToMaxRegions - Started.");

        OverloadedRegionsRemover overRegItr = new OverloadedRegionsRemover(
                serversByLoad.iterator(), max);

        ServersByLoadIterator serversByMinOrMaxLoadItr = new ServersByLoadIterator(
                serversByLoad.iterator(), min);

        ServerAndAllClusteredRegions dest = null;
        int limit = min;
        while (overRegItr.hasNext()) {
            if (!serversByMinOrMaxLoadItr.hasNext()) {
                serversByMinOrMaxLoadItr = new ServersByLoadIterator(
                        serversByLoad.iterator(), max);
                limit = max;
                if (!serversByMinOrMaxLoadItr.hasNext())
                    break;
            }
            if (dest == null || dest.getLoad() >= limit)
                dest = serversByMinOrMaxLoadItr.next();

            OverloadedRegionsRemover.TruncatedElement srcRegion = overRegItr
                    .next();
            dest.addCluster(srcRegion.regionsCluster);
            prepareRegionsPlans(srcRegion.regionsCluster, srcRegion.serverName,
                    dest.getServerName(), result);
        }

        LOG.info("truncateRegionServersToMaxRegions - Done.");
        return result;
    }

    private Map<HRegionInfo, RegionPlan> balanceRegionServersToMinRegions(
            List<ServerAndAllClusteredRegions> serversByLoad, int min) {
        Map<HRegionInfo, RegionPlan> result = new HashMap<HRegionInfo, RegionPlan>();

        LOG.info("balanceRegionServersToMinRegions - started.");

        ServersByLoadIterator serversByMinLoadItr = new ServersByLoadIterator(
                serversByLoad.iterator(), min);

        OverloadedRegionsRemover minTruncator = new OverloadedRegionsRemover(
                serversByLoad.iterator(), min);

        ServerAndAllClusteredRegions dest = null;
        while ((serversByMinLoadItr.hasNext() || (dest != null && dest
                .getLoad() < min)) && minTruncator.hasNext()) {
            if (dest == null || dest.getLoad() >= min)
                dest = serversByMinLoadItr.next();

            TruncatedElement srcRegion = minTruncator.next();
            dest.addCluster(srcRegion.regionsCluster);
            prepareRegionsPlans(srcRegion.regionsCluster, srcRegion.serverName,
                    dest.getServerName(), result);
        }

        LOG.info("balanceRegionServersToMinRegions - done.");

        return result;
    }

    private void prepareRegionsPlans(List<HRegionInfo> hriList, ServerName src,
            ServerName dest, Map<HRegionInfo, RegionPlan> result) {
        for (HRegionInfo hri : hriList) {
            result.put(hri, new RegionPlan(hri, src, dest));
        }
    }

    @Override
    public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
            List<HRegionInfo> regions, List<ServerName> servers) {
        // TODO Auto-generated method stub
        Map<ServerName, List<HRegionInfo>> result = new TreeMap<ServerName, List<HRegionInfo>>();

        if (regions == null || regions.isEmpty()) {
            LOG.info("Empty regions have been passed. Returning empty assignments.");
            return result;
        }
        if (servers == null || servers.isEmpty()) {
            LOG.info("Empty servers have been passed. Returning empty assignments.");
            return result;
        }

        LOG.info("Round robin assignment - started.");
        int numServers = servers.size();
        int numRegions = regions.size();
        int maxSize = (numRegions / numServers)
                + ((numRegions % numServers == 0) ? 0 : 1);
        List<List<HRegionInfo>> clusteredRegions = getValuesAsList(clusterRegions(regions));
        Iterator<List<HRegionInfo>> itr = clusteredRegions.iterator();
        for (ServerName server : servers) {
            while (itr.hasNext()) {
                List<HRegionInfo> input = itr.next();
                if (!result.containsKey(server))
                    result.put(server, new ArrayList<HRegionInfo>());
                result.get(server).addAll(input);
                if (result.get(server).size() >= maxSize)
                    break;
            }
        }

        LOG.info("Round robin assignment - done.");
        return result;
    }

    /**
     * This is called during the cluster initialization.
     * 
     * 1) Tries to retain the existing region servers and regions mappings 2) Makes sure related regions are placed on the same region server.
     * 
     * The algorithms is as following
     * 
     * <ol>
     * 
     * <li>Cluster the regions based on (key range, and related tables group id).
     * 
     * <li>For each clustered region group, figure out whether any existing hosting region server of the region is dead, or figure out if regions are placed on
     * different hosts. If yes, try to allocate a region server for the unplaced regions, and also move the existing regions to a region server where already
     * the majority of the regions are living. This step is handled by internal method.
     * 
     * </ol>
     */
    @Override
    public Map<ServerName, List<HRegionInfo>> retainAssignment(
            Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
        Map<ServerName, List<HRegionInfo>> result = new TreeMap<ServerName, List<HRegionInfo>>();
        if (servers == null || servers.isEmpty())
            return result;
        if (regions == null || regions.isEmpty())
            return result;

        LOG.info("retainAssignment - started.");

        int totReassignedCnt = 0;
        long startTime = System.currentTimeMillis();

        Set<ServerName> allUnavailServers = minus(regions.values(), servers);
        Collection<List<HRegionInfo>> allClusteredRegionGroups = getValuesAsList(clusterRegions(regions
                .keySet()));
        Map<String, List<ServerName>> hostNameAndAvaServers = clusterServers(servers);

        for (List<HRegionInfo> indClusteredRegionGroup : allClusteredRegionGroups) {
            ListMultimap<ServerName, HRegionInfo> indServerNameAndClusteredRegions = reverseMap(getMapEntriesForKeys(
                    regions, indClusteredRegionGroup));

            Set<ServerName> localAvailServers = intersect(
                    indServerNameAndClusteredRegions.keySet(), servers);

            String avaHostWithMajRegions = findAvailableHostWithMajorityRegions(
                    hostNameAndAvaServers.keySet(),
                    indServerNameAndClusteredRegions);
            ServerName snToUseOnNoLocalAvaServers = (avaHostWithMajRegions == null) ? (randomAssignment(servers))
                    : randomAssignment(hostNameAndAvaServers
                            .get(avaHostWithMajRegions));
            boolean isNoLocalServerAvailable = localAvailServers.size() == 0;

            ServerName bestPlacementServer = (isNoLocalServerAvailable) ? (snToUseOnNoLocalAvaServers)
                    : findAvailServerNameWithMajorityRegions(localAvailServers,
                            indServerNameAndClusteredRegions);

            if (!result.containsKey(bestPlacementServer))
                result.put(bestPlacementServer, new ArrayList<HRegionInfo>());
            for (ServerName sn : indServerNameAndClusteredRegions.keySet()) {
                List<HRegionInfo> regionsToAssign = indServerNameAndClusteredRegions
                        .get(sn);
                result.get(bestPlacementServer).addAll(regionsToAssign);
                if (!(sn.equals(bestPlacementServer)))
                    totReassignedCnt += regionsToAssign.size();
            }

        }
        long endTime = System.currentTimeMillis();
        LOG.info("No of unavailable servers : " + allUnavailServers.size()
                + " and no of available servers : " + servers.size()
                + " and unavailable servers are: \n"
                + Joiner.on("\n").join(allUnavailServers));

        LOG.info("Total no of reassigned regions : " + totReassignedCnt);
        LOG.info("Total time took for retain assignment (in ms) :"
                + (endTime - startTime));
        return result;
    }

    private String findAvailableHostWithMajorityRegions(
            Set<String> availableHosts,
            ListMultimap<ServerName, HRegionInfo> localServerNameAndClusteredRegions) {
        int maxRegionsCount = -1;
        String result = null;
        for (ServerName sn : localServerNameAndClusteredRegions.keySet()) {
            if (availableHosts.contains(sn.getHostname())) {
                if (localServerNameAndClusteredRegions.get(sn).size() > maxRegionsCount) {
                    result = sn.getHostname();
                    maxRegionsCount = localServerNameAndClusteredRegions
                            .get(sn).size();
                }
            }
        }
        return result;
    }

    /**
     * Returns the server which hosts many regions. This function is mainly used to find server name to host related regions. It makes sense to place a related
     * region on a host where already sister regions are living.
     * 
     * @param serverNameAndRegionsMap
     * @return
     */
    private ServerName findAvailServerNameWithMajorityRegions(
            Set<ServerName> availableServers,
            ListMultimap<ServerName, HRegionInfo> serverNameAndRegionsMap) {
        ServerName result = null;
        int maxRegionsCount = -1;
        for (ServerName serverName : serverNameAndRegionsMap.keySet()) {
            if (availableServers.contains(serverName)) {
                if (serverNameAndRegionsMap.get(serverName).size() > maxRegionsCount) {
                    maxRegionsCount = serverNameAndRegionsMap.get(serverName)
                            .size();
                    result = serverName;
                }
            }
        }

        return result;
    }

    /**
     * Immediate assignment of regions to servers. Does not consider the best way to assign regions. Makes sure related regions are assigned to the same region
     * server.
     */
    @Override
    public Map<HRegionInfo, ServerName> immediateAssignment(
            List<HRegionInfo> regions, List<ServerName> servers) {
        Map<HRegionInfo, ServerName> assignments = new TreeMap<HRegionInfo, ServerName>();

        if (regions == null || regions.isEmpty()) {
            LOG.info("Empty regions have been passed. Returning empty assignments.");
            return assignments;
        }
        if (servers == null || servers.isEmpty()) {
            LOG.info("Empty servers have been passed. Returning empty assignments.");
            return assignments;
        }

        LOG.info("immediateAssignment started.");

        List<List<HRegionInfo>> clusteredRegionGroups = getValuesAsList(clusterRegions(regions));

        for (List<HRegionInfo> clusterRegionGroup : clusteredRegionGroups) {
            ServerName randomServer = randomAssignment(servers);
            for (HRegionInfo clusterRegion : clusterRegionGroup)
                assignments.put(clusterRegion, randomServer);
        }

        LOG.info("immediateAssignment finished.");
        return assignments;
    }

    @Override
    public ServerName randomAssignment(List<ServerName> servers) {
        if (servers == null || servers.isEmpty())
            return null;
        return servers.get(RANDOM.nextInt(servers.size()));
    }

    /**
     * This function is supposed to be used by HBase#0.98 {@link LoadBalancer}
     * 
     * @param regionInfo
     * @param servers
     * @return
     */
    public ServerName randomAssignment(HRegionInfo regionInfo,
            List<ServerName> servers) {

        Set<ServerName> sortedServers = new TreeSet<ServerName>(servers);
        RegionClusterKey regionClusterKey = new RegionClusterKey(
                this.tableToClusterMapObj.getClusterName(regionInfo
                        .getTableNameAsString()), regionInfo.getStartKey(),
                regionInfo.getEndKey());
        ServerName random = randomAssignment(servers);
        ServerName result = regionsAndServers.putIfAbsent(regionClusterKey,
                random);
        boolean temp = false;
        while (result != null && !sortedServers.contains(result) && !temp) {
            temp = regionsAndServers.replace(regionClusterKey, result, random);
            result = regionsAndServers.get(regionClusterKey);
        }
        return regionsAndServers.get(regionClusterKey);
    }

    /**
     * Groups the regions based on the region start key and region end key.
     * 
     * @param regions
     * @return
     */
    public Map<RegionClusterKey, List<HRegionInfo>> clusterRegions(
            Collection<HRegionInfo> regions) {
        return cluster(regions, regionKeyGener);
    }

    public Map<String, List<ServerName>> clusterServers(
            Collection<ServerName> servers) {
        return cluster(servers, hostKeyGener);
    }

    public void stop() {
        if (fileChangeListener != null && fileChangeListenerThread != null
                && fileChangeListenerThread.isAlive()) {
            fileChangeListener.requestStop();
            fileChangeListenerThread.interrupt();
        }
    }

    public void moveRegionsParallel(final List<RegionPlan> regionPlans)
            throws IOException, InterruptedException {
        final HBaseAdmin admin = new HBaseAdmin(conf);
        final HTable hMetaTable = new HTable(HConstants.META_TABLE_NAME,
                admin.getConnection());

        final RegionMoverImpl regionMover = new RegionMoverImpl(admin,
                hMetaTable, maxTimeToWaitForRegionMove);
        ParallelRegionMover pRegionMover = new ParallelRegionMover(regionMover,
                noOfMoverThreads, executorService);
        pRegionMover.moveRegionsParallel(regionPlans);
        hMetaTable.close();
        admin.close();
    }

}