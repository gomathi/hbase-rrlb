package org.balancer.test.rrlbalancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.balancer.rrlbalancer.RelatedRegionsLoadBalancer;
import org.balancer.rrlbalancer.Utils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * NOTE: This code is copied from HBase#TestDefaultLoadBalancer class, and have been modified to test {@link RelatedRegionsLoadBalancer}, and also has few extra
 * test cases to test RRL balancer specifically.
 * 
 * Start at {@link #testAlreadyBalancedClusterRRL} to review the code.
 */
public class TestRelatedRegionsLoadBalancer {
    private static final Log LOG = LogFactory
            .getLog(TestRelatedRegionsLoadBalancer.class);

    private static LoadBalancer loadBalancer;

    private static Random rand;

    @BeforeClass
    public static void beforeAllTests() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.regions.slop", "0");
        loadBalancer = new RelatedRegionsLoadBalancer(null, false);
        loadBalancer.setConf(conf);
        rand = new Random();
    }

    // int[testnum][servernumber] -> numregions
    int[][] clusterStateMocks = new int[][] {
            // 1 node
            new int[] { 0 },
            new int[] { 1 },
            new int[] { 10 },
            // 2 node
            new int[] { 0, 0 },
            new int[] { 2, 0 },
            new int[] { 2, 1 },
            new int[] { 2, 2 },
            new int[] { 2, 3 },
            new int[] { 2, 4 },
            new int[] { 1, 1 },
            new int[] { 0, 1 },
            new int[] { 10, 1 },
            new int[] { 14, 1432 },
            new int[] { 47, 53 },
            // 3 node
            new int[] { 0, 1, 2 },
            new int[] { 1, 2, 3 },
            new int[] { 0, 2, 2 },
            new int[] { 0, 3, 0 },
            new int[] { 0, 4, 0 },
            new int[] { 20, 20, 0 },
            // 4 node
            new int[] { 0, 1, 2, 3 },
            new int[] { 4, 0, 0, 0 },
            new int[] { 5, 0, 0, 0 },
            new int[] { 6, 6, 0, 0 },
            new int[] { 6, 2, 0, 0 },
            new int[] { 6, 1, 0, 0 },
            new int[] { 6, 0, 0, 0 },
            new int[] { 4, 4, 4, 7 },
            new int[] { 4, 4, 4, 8 },
            new int[] { 0, 0, 0, 7 },
            // 5 node
            new int[] { 1, 1, 1, 1, 4 },
            // more nodes
            new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 },
            new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 10 },
            new int[] { 6, 6, 5, 6, 6, 6, 6, 6, 6, 1 },
            new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 54 },
            new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 55 },
            new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 56 },
            new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 16 },
            new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 8 },
            new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 9 },
            new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 10 },
            new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 123 },
            new int[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 155 },
            new int[] { 0, 0, 144, 1, 1, 1, 1, 1123, 133, 138, 12, 1444 },
            new int[] { 0, 0, 144, 1, 0, 4, 1, 1123, 133, 138, 12, 1444 },
            new int[] { 1538, 1392, 1561, 1557, 1535, 1553, 1385, 1542, 1619 } };

    int[][] regionsAndServersMocks = new int[][] {
            // { num regions, num servers }
            new int[] { 0, 0 }, new int[] { 0, 1 }, new int[] { 1, 1 },
            new int[] { 2, 1 }, new int[] { 10, 1 }, new int[] { 1, 2 },
            new int[] { 2, 2 }, new int[] { 3, 2 }, new int[] { 1, 3 },
            new int[] { 2, 3 }, new int[] { 3, 3 }, new int[] { 25, 3 },
            new int[] { 2, 10 }, new int[] { 2, 100 }, new int[] { 12, 10 },
            new int[] { 12, 100 }, };

    /**
     * Test the load balancing algorithm.
     * 
     * Invariant is that all servers should be hosting either floor(average) or ceiling(average)
     * 
     * @throws Exception
     */
    @Test
    public void testBalanceCluster() throws Exception {

        for (int[] mockCluster : clusterStateMocks) {
            Map<ServerName, List<HRegionInfo>> servers = mockClusterServers(mockCluster);
            List<TestServerAndLoad> list = convertToList(servers);
            LOG.info("Mock Cluster : " + printMock(list) + " "
                    + printStats(list));
            List<RegionPlan> plans = loadBalancer.balanceCluster(servers);
            List<TestServerAndLoad> balancedCluster = reconcile(list, plans);
            LOG.info("Mock Balance : " + printMock(balancedCluster));
            assertClusterAsBalanced(balancedCluster);
            for (Map.Entry<ServerName, List<HRegionInfo>> entry : servers
                    .entrySet()) {
                returnRegions(entry.getValue());
                returnServer(entry.getKey());
            }
        }

    }

    /**
     * Invariant is that all servers have between floor(avg) and ceiling(avg) number of regions.
     */
    public void assertClusterAsBalanced(List<TestServerAndLoad> servers) {
        int numServers = servers.size();
        int numRegions = 0;
        int maxRegions = 0;
        int minRegions = Integer.MAX_VALUE;
        for (TestServerAndLoad server : servers) {
            int nr = server.getLoad();
            if (nr > maxRegions) {
                maxRegions = nr;
            }
            if (nr < minRegions) {
                minRegions = nr;
            }
            numRegions += nr;
        }
        if (maxRegions - minRegions < 2) {
            // less than 2 between max and min, can't balance
            return;
        }
        int min = numRegions / numServers;
        int max = numRegions % numServers == 0 ? min : min + 1;

        for (TestServerAndLoad server : servers) {
            assertTrue(server.getLoad() <= max);
            assertTrue(server.getLoad() >= min);
        }
    }

    /**
     * Tests immediate assignment.
     * 
     * Invariant is that all regions have an assignment.
     * 
     * @throws Exception
     */
    @Test
    public void testImmediateAssignment() throws Exception {
        for (int[] mock : regionsAndServersMocks) {
            LOG.debug("testImmediateAssignment with " + mock[0]
                    + " regions and " + mock[1] + " servers");
            List<HRegionInfo> regions = randomRegions(mock[0]);
            List<TestServerAndLoad> servers = randomServers(mock[1], 0);
            List<ServerName> list = getListOfServerNames(servers);
            Map<HRegionInfo, ServerName> assignments = loadBalancer
                    .immediateAssignment(regions, list);
            assertImmediateAssignment(regions, list, assignments);
            returnRegions(regions);
            returnServers(list);
        }
    }

    /**
     * All regions have an assignment.
     * 
     * @param regions
     * @param servers
     * @param assignments
     */
    private void assertImmediateAssignment(List<HRegionInfo> regions,
            List<ServerName> servers, Map<HRegionInfo, ServerName> assignments) {
        for (HRegionInfo region : regions) {
            assertTrue(assignments.containsKey(region));
        }
    }

    /**
     * Tests the bulk assignment used during cluster startup.
     * 
     * Round-robin. Should yield a balanced cluster so same invariant as the load balancer holds, all servers holding either floor(avg) or ceiling(avg).
     * 
     * @throws Exception
     */
    @Test
    public void testBulkAssignment() throws Exception {
        for (int[] mock : regionsAndServersMocks) {
            LOG.debug("testBulkAssignment with " + mock[0] + " regions and "
                    + mock[1] + " servers");
            List<HRegionInfo> regions = randomRegions(mock[0]);
            List<TestServerAndLoad> servers = randomServers(mock[1], 0);
            List<ServerName> list = getListOfServerNames(servers);
            Map<ServerName, List<HRegionInfo>> assignments = loadBalancer
                    .roundRobinAssignment(regions, list);
            float average = (float) regions.size() / servers.size();
            int min = (int) Math.floor(average);
            int max = (int) Math.ceil(average);
            if (assignments != null && !assignments.isEmpty()) {
                for (List<HRegionInfo> regionList : assignments.values()) {
                    assertTrue(regionList.size() >= min
                            || regionList.size() <= max);
                }
            }
            returnRegions(regions);
            returnServers(list);
        }
    }

    /**
     * Test the cluster startup bulk assignment which attempts to retain assignment info.
     * 
     * @throws Exception
     */
    @Test
    public void testRetainAssignment() throws Exception {
        // Test simple case where all same servers are there
        List<TestServerAndLoad> servers = randomServers(10, 10);
        List<HRegionInfo> regions = randomRegions(100);
        Map<HRegionInfo, ServerName> existing = new TreeMap<HRegionInfo, ServerName>();
        for (int i = 0; i < regions.size(); i++) {
            ServerName sn = servers.get(i % servers.size()).getServerName();
            // The old server would have had same host and port, but different
            // start code!
            ServerName snWithOldStartCode = new ServerName(sn.getHostname(),
                    sn.getPort(), sn.getStartcode() - 10);
            existing.put(regions.get(i), snWithOldStartCode);
        }
        List<ServerName> listOfServerNames = getListOfServerNames(servers);
        Map<ServerName, List<HRegionInfo>> assignment = loadBalancer
                .retainAssignment(existing, listOfServerNames);
        assertRetainedAssignment(existing, listOfServerNames, assignment);

        // Include two new servers that were not there before
        List<TestServerAndLoad> servers2 = new ArrayList<TestServerAndLoad>(
                servers);
        servers2.add(randomServer(10));
        servers2.add(randomServer(10));
        listOfServerNames = getListOfServerNames(servers2);
        assignment = loadBalancer.retainAssignment(existing, listOfServerNames);
        assertRetainedAssignment(existing, listOfServerNames, assignment);

        // Remove two of the servers that were previously there
        List<TestServerAndLoad> servers3 = new ArrayList<TestServerAndLoad>(
                servers);
        servers3.remove(0);
        servers3.remove(0);
        listOfServerNames = getListOfServerNames(servers3);
        assignment = loadBalancer.retainAssignment(existing, listOfServerNames);
        assertRetainedAssignment(existing, listOfServerNames, assignment);
    }

    private List<ServerName> getListOfServerNames(
            final List<TestServerAndLoad> sals) {
        List<ServerName> list = new ArrayList<ServerName>();
        for (TestServerAndLoad e : sals) {
            list.add(e.getServerName());
        }
        return list;
    }

    /**
     * Asserts a valid retained assignment plan.
     * <p>
     * Must meet the following conditions:
     * <ul>
     * <li>Every input region has an assignment, and to an online server
     * <li>If a region had an existing assignment to a server with the same address a a currently online server, it will be assigned to it
     * </ul>
     * 
     * @param existing
     * @param servers
     * @param assignment
     */
    private void assertRetainedAssignment(
            Map<HRegionInfo, ServerName> existing, List<ServerName> servers,
            Map<ServerName, List<HRegionInfo>> assignment) {
        // Verify condition 1, every region assigned, and to online server
        Set<ServerName> onlineServerSet = new TreeSet<ServerName>(servers);
        Set<HRegionInfo> assignedRegions = new TreeSet<HRegionInfo>();
        for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
            assertTrue(
                    "Region assigned to server that was not listed as online",
                    onlineServerSet.contains(a.getKey()));
            for (HRegionInfo r : a.getValue())
                assignedRegions.add(r);
        }
        assertEquals(existing.size(), assignedRegions.size());

        // Verify condition 2, if server had existing assignment, must have same
        Set<String> onlineHostNames = new TreeSet<String>();
        for (ServerName s : servers) {
            onlineHostNames.add(s.getHostname());
        }

        for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
            ServerName assignedTo = a.getKey();
            for (HRegionInfo r : a.getValue()) {
                ServerName address = existing.get(r);
                if (address != null
                        && onlineHostNames.contains(address.getHostname())) {
                    // this region was prevously assigned somewhere, and that
                    // host is still around, then it should be re-assigned on
                    // the
                    // same host
                    assertEquals(address.getHostname(),
                            assignedTo.getHostname());
                }
            }
        }
    }

    private String printStats(List<TestServerAndLoad> servers) {
        int numServers = servers.size();
        int totalRegions = 0;
        for (TestServerAndLoad server : servers) {
            totalRegions += server.getLoad();
        }
        float average = (float) totalRegions / numServers;
        int max = (int) Math.ceil(average);
        int min = (int) Math.floor(average);
        return "[srvr=" + numServers + " rgns=" + totalRegions + " avg="
                + average + " max=" + max + " min=" + min + "]";
    }

    private List<TestServerAndLoad> convertToList(
            final Map<ServerName, List<HRegionInfo>> servers) {
        List<TestServerAndLoad> list = new ArrayList<TestServerAndLoad>(
                servers.size());
        for (Map.Entry<ServerName, List<HRegionInfo>> e : servers.entrySet()) {
            list.add(new TestServerAndLoad(e.getKey(), e.getValue().size()));
        }
        return list;
    }

    private String printMock(List<TestServerAndLoad> balancedCluster) {
        SortedSet<TestServerAndLoad> sorted = new TreeSet<TestServerAndLoad>(
                balancedCluster);
        TestServerAndLoad[] arr = sorted.toArray(new TestServerAndLoad[sorted
                .size()]);
        StringBuilder sb = new StringBuilder(sorted.size() * 4 + 4);
        sb.append("{ ");
        for (int i = 0; i < arr.length; i++) {
            if (i != 0) {
                sb.append(" , ");
            }
            sb.append(arr[i].getLoad());
        }
        sb.append(" }");
        return sb.toString();
    }

    /**
     * This assumes the RegionPlan HSI instances are the same ones in the map, so actually no need to even pass in the map, but I think it's clearer.
     * 
     * @param list
     * @param plans
     * @return
     */
    private List<TestServerAndLoad> reconcile(List<TestServerAndLoad> list,
            List<RegionPlan> plans) {
        List<TestServerAndLoad> result = new ArrayList<TestServerAndLoad>(
                list.size());
        if (plans == null)
            return result;
        Map<ServerName, TestServerAndLoad> map = new HashMap<ServerName, TestServerAndLoad>(
                list.size());
        for (RegionPlan plan : plans) {
            ServerName source = plan.getSource();
            updateLoad(map, source, -1);
            ServerName destination = plan.getDestination();
            updateLoad(map, destination, +1);
        }
        result.clear();
        result.addAll(map.values());
        return result;
    }

    private void updateLoad(Map<ServerName, TestServerAndLoad> map,
            final ServerName sn, final int diff) {
        TestServerAndLoad sal = map.get(sn);
        if (sal == null)
            return;
        sal = new TestServerAndLoad(sn, sal.getLoad() + diff);
        map.put(sn, sal);
    }

    private Map<ServerName, List<HRegionInfo>> mockClusterServers(
            int[] mockCluster) {
        int numServers = mockCluster.length;
        Map<ServerName, List<HRegionInfo>> servers = new TreeMap<ServerName, List<HRegionInfo>>();
        for (int i = 0; i < numServers; i++) {
            int numRegions = mockCluster[i];
            TestServerAndLoad sal = randomServer(0);
            List<HRegionInfo> regions = randomRegions(numRegions);
            servers.put(sal.getServerName(), regions);
        }
        return servers;
    }

    private Queue<HRegionInfo> regionQueue = new LinkedList<HRegionInfo>();
    static int regionId = 0;

    private List<HRegionInfo> randomRegions(int numRegions) {
        List<HRegionInfo> regions = new ArrayList<HRegionInfo>(numRegions);
        byte[] start = new byte[16];
        byte[] end = new byte[16];
        rand.nextBytes(start);
        rand.nextBytes(end);
        for (int i = 0; i < numRegions; i++) {
            if (!regionQueue.isEmpty()) {
                regions.add(regionQueue.poll());
                continue;
            }
            Bytes.putInt(start, 0, numRegions << 1);
            Bytes.putInt(end, 0, (numRegions << 1) + 1);
            HRegionInfo hri = new HRegionInfo(Bytes.toBytes("table" + i),
                    start, end, false, regionId++);
            regions.add(hri);
        }
        return regions;
    }

    private void returnRegions(List<HRegionInfo> regions) {
        regionQueue.addAll(regions);
    }

    private Queue<ServerName> serverQueue = new LinkedList<ServerName>();

    private TestServerAndLoad randomServer(final int numRegionsPerServer) {
        if (!this.serverQueue.isEmpty()) {
            ServerName sn = this.serverQueue.poll();
            return new TestServerAndLoad(sn, numRegionsPerServer);
        }
        String host = "server" + rand.nextInt(100000);
        int port = rand.nextInt(60000);
        long startCode = rand.nextLong();
        ServerName sn = new ServerName(host, port, startCode);
        return new TestServerAndLoad(sn, numRegionsPerServer);
    }

    private List<TestServerAndLoad> randomServers(int numServers,
            int numRegionsPerServer) {
        List<TestServerAndLoad> servers = new ArrayList<TestServerAndLoad>(
                numServers);
        for (int i = 0; i < numServers; i++) {
            servers.add(randomServer(numRegionsPerServer));
        }
        return servers;
    }

    private void returnServer(ServerName server) {
        serverQueue.add(server);
    }

    private void returnServers(List<ServerName> servers) {
        this.serverQueue.addAll(servers);
    }

    // Related regions load balancer test cases

    private static final List<String> regionKeys = new ArrayList<String>();

    static {
        for (int i = 'a'; i <= 'z'; i++)
            regionKeys.add(String.valueOf((char) i));
        int ind = 0;
        for (int j = 0; j < 76; j++) {
            for (int i = 'a'; i <= 'z'; i++) {
                regionKeys.add(regionKeys.get(ind) + (char) i);
                ind++;
            }
        }
        Collections.sort(regionKeys);
    }

    // int[testcase]([])
    // each input [r,t,a,ts]
    // r -> no of regions per table
    // t -> no of tables
    // a -> available servers count
    // t -> total no of servers
    private static final int[][] rrlInput = new int[][] {
            new int[] { 1, 1, 1, 1 }, new int[] { 4, 2, 2, 2 },
            new int[] { 4, 2, 2, 4 }, new int[] { 4, 2, 2, 10 },
            new int[] { 1000, 6, 10, 20 }, new int[] { 1000, 6, 10, 17 } };

    @Test
    public void testAlreadyBalancedClusterRRL() {
        List<Set<String>> relatedTablesList = new ArrayList<Set<String>>();
        Set<String> relatedTables = new HashSet<String>();
        relatedTablesList.add(relatedTables);
        List<HRegionInfo> regions = genRegions(4, 2, relatedTables);
        List<TestServerAndLoad> servers = randomServers(2, 4);

        Map<ServerName, List<HRegionInfo>> input = assignServersToRegions(
                regions, servers, false);

        RelatedRegionsLoadBalancer loadBal = new RelatedRegionsLoadBalancer(
                relatedTablesList, false);
        List<RegionPlan> result = loadBal.balanceCluster(input);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testImmediateAssignmentRRL() {
        List<Set<String>> relatedTablesList = new ArrayList<Set<String>>();
        Set<String> relatedTables = new HashSet<String>();
        relatedTablesList.add(relatedTables);
        List<HRegionInfo> regions = genRegions(4, 2, relatedTables);
        List<TestServerAndLoad> servers = randomServers(2, 0);
        List<ServerName> serverNames = new ArrayList<ServerName>();
        for (TestServerAndLoad tsal : servers)
            serverNames.add(tsal.getServerName());

        RelatedRegionsLoadBalancer loadBal = new RelatedRegionsLoadBalancer(
                relatedTablesList, false);
        Map<HRegionInfo, ServerName> result = loadBal.immediateAssignment(
                regions, serverNames);

        assertEquals(8, result.size());
        List<List<HRegionInfo>> clusteredRegions = Utils
                .getValuesAsList(loadBal.clusterRegions(result.keySet()));

        for (List<HRegionInfo> cluster : clusteredRegions) {
            if (cluster.size() > 0) {
                ServerName sn = result.get(cluster.get(0));
                for (HRegionInfo hri : cluster) {
                    assertEquals(sn, result.get(hri));
                }
            }
        }
    }

    @Test
    public void testRoundRobinAssignmentRRL() {
        List<Set<String>> relatedTablesList = new ArrayList<Set<String>>();
        Set<String> relatedTables = new HashSet<String>();
        relatedTablesList.add(relatedTables);
        List<HRegionInfo> regions = genRegions(4, 2, relatedTables);
        List<TestServerAndLoad> servers = randomServers(2, 0);
        List<ServerName> serverNames = new ArrayList<ServerName>();
        for (TestServerAndLoad tsal : servers)
            serverNames.add(tsal.getServerName());

        RelatedRegionsLoadBalancer loadBal = new RelatedRegionsLoadBalancer(
                relatedTablesList, false);

        Map<ServerName, List<HRegionInfo>> result = loadBal
                .roundRobinAssignment(regions, serverNames);
        assertEquals(2, result.size());
        for (Map.Entry<ServerName, List<HRegionInfo>> entry : result.entrySet()) {
            List<List<HRegionInfo>> clusteredRegions = Utils
                    .getValuesAsList(loadBal.clusterRegions(entry.getValue()));
            for (List<HRegionInfo> cluster : clusteredRegions)
                assertEquals(2, cluster.size());
        }
    }

    @Test
    public void testBalanceClusterRRL() {
        for (int[] rrlIndTestInput : rrlInput) {

            int noOfRegionsPerTable = rrlIndTestInput[0];
            int noOfTables = rrlIndTestInput[1];
            int noOfServersToAssign = rrlIndTestInput[2];
            int totNoOfServers = rrlIndTestInput[3];
            int noOfAddServers = totNoOfServers - noOfServersToAssign;
            int totRegions = (noOfRegionsPerTable * noOfTables);
            int noOfRegionsPerServer = (totRegions / noOfServersToAssign)
                    + ((totRegions % noOfServersToAssign) == 0 ? 0 : 1);

            Set<String> relatedTables = new HashSet<String>();
            List<Set<String>> relatedTablesList = new ArrayList<Set<String>>();
            relatedTablesList.add(relatedTables);

            List<HRegionInfo> hregionsList = genRegions(noOfRegionsPerTable,
                    noOfTables, relatedTables);
            List<TestServerAndLoad> serversList = randomServers(
                    noOfServersToAssign, noOfRegionsPerServer);
            Map<ServerName, List<HRegionInfo>> input = assignServersToRegions(
                    hregionsList, serversList, true);
            if (noOfAddServers > 0) {
                List<TestServerAndLoad> additionalServers = randomServers(
                        noOfAddServers, 0);
                for (TestServerAndLoad tsal : additionalServers) {
                    input.put(tsal.getServerName(),
                            new ArrayList<HRegionInfo>());
                }
            }

            Map<ServerName, List<HRegionInfo>> cInput = new HashMap<ServerName, List<HRegionInfo>>(
                    input);
            RelatedRegionsLoadBalancer loadBal = new RelatedRegionsLoadBalancer(
                    relatedTablesList, false);
            List<RegionPlan> rplansList = loadBal.balanceCluster(input);
            int min = noOfRegionsPerTable / totNoOfServers;
            int max = (noOfRegionsPerTable % totNoOfServers) == 0 ? min
                    : (min + 1);
            assertTestBalanceClusterRRL(rplansList, cInput, loadBal,
                    noOfTables, min, max);
        }
    }

    @Test
    public void testWeightedBalancing() {
        Map<String, Integer> hostNameAndWeight = new HashMap<String, Integer>();
        String hostA = "hosta";
        String hostB = "hostb";
        String hostC = "hostc";
        hostNameAndWeight.put(hostA, 1);
        hostNameAndWeight.put(hostB, 2);
        hostNameAndWeight.put(hostC, 3);

        ServerName serverA = new ServerName(hostA, 40000, 45);
        ServerName serverB = new ServerName(hostB, 50000, 45);
        ServerName serverC = new ServerName(hostC, 50000, 45);

        List<Set<String>> relatedTablesList = new ArrayList<Set<String>>();
        Set<String> relatedTables = new HashSet<String>();
        relatedTablesList.add(relatedTables);
        List<HRegionInfo> regions = genRegions(18, 2, relatedTables);

        RelatedRegionsLoadBalancer loadBal = new RelatedRegionsLoadBalancer(
                relatedTablesList, hostNameAndWeight, false);
        Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
        clusterState.put(serverA, regions);
        clusterState.put(serverB, new ArrayList<HRegionInfo>());
        clusterState.put(serverC, new ArrayList<HRegionInfo>());

        List<RegionPlan> result = loadBal.balanceCluster(clusterState);
        assertTrue(!result.isEmpty());
        assertTrue(result.size() >= 30);

        Map<ServerName, List<HRegionInfo>> convertedResult = convertRegionPlanToResult(
                result, clusterState);
        assertEquals(3, loadBal.clusterRegions(convertedResult.get(serverA))
                .size());
        assertEquals(6, loadBal.clusterRegions(convertedResult.get(serverB))
                .size());
        assertEquals(9, loadBal.clusterRegions(convertedResult.get(serverC))
                .size());
    }

    @Test
    public void testWeightedBalancingDiff() throws InterruptedException {
        Map<String, Integer> hostNameAndWeight = new HashMap<String, Integer>();
        String hostA = "hosta";
        String hostB = "hostb";
        String hostC = "hostc";
        hostNameAndWeight.put(hostA, 1);
        hostNameAndWeight.put(hostB, 2);
        hostNameAndWeight.put(hostC, 4);

        ServerName serverA = new ServerName(hostA, 40000, 45);
        ServerName serverB = new ServerName(hostB, 50000, 45);
        ServerName serverC = new ServerName(hostC, 50000, 45);

        List<Set<String>> relatedTablesList = new ArrayList<Set<String>>();
        Set<String> relatedTables = new HashSet<String>();
        relatedTablesList.add(relatedTables);
        List<HRegionInfo> regions = genRegions(18, 2, relatedTables);

        RelatedRegionsLoadBalancer loadBal = new RelatedRegionsLoadBalancer(
                relatedTablesList, hostNameAndWeight, false);
        Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
        clusterState.put(serverA, regions);
        clusterState.put(serverB, new ArrayList<HRegionInfo>());
        clusterState.put(serverC, new ArrayList<HRegionInfo>());

        List<RegionPlan> result = loadBal.balanceCluster(clusterState);
        Map<ServerName, List<HRegionInfo>> convertedResult = convertRegionPlanToResult(
                result, clusterState);

        assertTrue(loadBal.clusterRegions(convertedResult.get(serverA)).size() >= 2
                && loadBal.clusterRegions(convertedResult.get(serverA)).size() <= 3);
        assertTrue(loadBal.clusterRegions(convertedResult.get(serverB)).size() >= 4
                && loadBal.clusterRegions(convertedResult.get(serverB)).size() <= 6);
        assertTrue(loadBal.clusterRegions(convertedResult.get(serverC)).size() >= 8
                && loadBal.clusterRegions(convertedResult.get(serverC)).size() <= 12);

        result = loadBal.balanceCluster(convertedResult);
        Assert.assertTrue(result.size() == 0);
    }

    @Test
    public void testRandomAssignmentRRL() {

        Set<String> relatedTables = new HashSet<String>();
        List<Set<String>> relatedTablesList = new ArrayList<Set<String>>();
        relatedTablesList.add(relatedTables);

        List<HRegionInfo> regionsFirst = genRegions(1, 4, relatedTables);

        List<TestServerAndLoad> servers = randomServers(5, 0);
        List<ServerName> serversList = new ArrayList<ServerName>();
        for (TestServerAndLoad server : servers)
            serversList.add(server.getServerName());

        RelatedRegionsLoadBalancer loadBal = new RelatedRegionsLoadBalancer(
                relatedTablesList, false);

        ServerName assServer = loadBal.randomAssignment(regionsFirst.get(0),
                serversList);
        ServerName assServerTwo = loadBal.randomAssignment(regionsFirst.get(1),
                serversList);

        assertEquals(assServer, assServerTwo);
        serversList.remove(assServerTwo);

        ServerName assServerThree = loadBal.randomAssignment(
                regionsFirst.get(2), serversList);
        ServerName assServerFour = loadBal.randomAssignment(
                regionsFirst.get(3), serversList);
        assertTrue(!assServerThree.equals(assServerTwo));
        assertEquals(assServerThree, assServerFour);
    }

    private void assertTestBalanceClusterRRL(List<RegionPlan> rplansList,
            Map<ServerName, List<HRegionInfo>> input,
            RelatedRegionsLoadBalancer loadBal, int clusterSize, int minRegCnt,
            int maxRegCnt) {
        Map<ServerName, List<HRegionInfo>> result = convertRegionPlanToResult(
                rplansList, input);
        for (List<HRegionInfo> list : result.values()) {
            List<List<HRegionInfo>> clusteredRegions = Utils
                    .getValuesAsList(loadBal.clusterRegions(list));
            assertTrue(clusteredRegions.size() >= minRegCnt
                    && clusteredRegions.size() <= maxRegCnt);
            for (List<HRegionInfo> cluster : clusteredRegions)
                assertEquals(clusterSize, cluster.size());
        }
    }

    private static Map<ServerName, List<HRegionInfo>> convertRegionPlanToResult(
            List<RegionPlan> rplansList,
            Map<ServerName, List<HRegionInfo>> input) {
        Map<ServerName, List<HRegionInfo>> result = new HashMap<ServerName, List<HRegionInfo>>();
        for (RegionPlan rp : rplansList) {
            if (!result.containsKey(rp.getDestination()))
                result.put(rp.getDestination(), new ArrayList<HRegionInfo>());
            result.get(rp.getDestination()).add(rp.getRegionInfo());
        }

        Set<HRegionInfo> hriList = new HashSet<HRegionInfo>();
        for (RegionPlan rp : rplansList)
            hriList.add(rp.getRegionInfo());

        for (Map.Entry<ServerName, List<HRegionInfo>> entry : input.entrySet()) {
            for (HRegionInfo hri : entry.getValue())
                if (!hriList.contains(hri)) {
                    if (!result.containsKey(entry.getKey()))
                        result.put(entry.getKey(), new ArrayList<HRegionInfo>());
                    result.get(entry.getKey()).add(hri);
                }

        }

        return result;
    }

    private Map<ServerName, List<HRegionInfo>> assignServersToRegions(
            List<HRegionInfo> regions, List<TestServerAndLoad> servers,
            boolean randomAssign) {
        Map<ServerName, List<HRegionInfo>> result = new HashMap<ServerName, List<HRegionInfo>>();
        for (TestServerAndLoad tsl : servers)
            result.put(tsl.getServerName(), new ArrayList<HRegionInfo>());

        if (randomAssign)
            Collections.shuffle(regions);
        Iterator<TestServerAndLoad> itr = servers.iterator();
        TestServerAndLoad currServerName = itr.next();
        int currServerLoad = currServerName.getLoad();
        for (int i = regions.size() - 1; i >= 0; i--) {
            if (currServerLoad == 0) {
                if (itr.hasNext()) {
                    currServerName = itr.next();
                    currServerLoad = currServerName.getLoad();
                }
                else
                    break;
            }
            currServerLoad--;
            result.get(currServerName.getServerName()).add(regions.get(i));
        }

        return result;
    }

    private List<HRegionInfo> genRegions(int numRegions, int noTables,
            Set<String> relatedTables) {
        for (int j = 0; j < noTables; j++)
            relatedTables.add("Table" + j);
        int ind = 0;
        List<HRegionInfo> result = new ArrayList<HRegionInfo>();
        for (int i = 0; i < numRegions; i++) {
            byte[] startKey = Bytes.toBytes(regionKeys.get(ind++));
            byte[] endKey = Bytes.toBytes(regionKeys.get(ind++));

            for (String tableName : relatedTables) {
                HRegionInfo hri = new HRegionInfo(Bytes.toBytes(tableName),
                        startKey, endKey);
                result.add(hri);
            }
        }
        return result;
    }

}
