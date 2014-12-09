package org.balancer.rrlbalancer;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;

public class RegionMoverImpl implements RegionMoverInterface {

    private final HBaseAdmin admin;
    private final HTable hMetaTable;
    private final long maxTimeToWaitForRegionMove;

    public RegionMoverImpl(
            final HBaseAdmin admin, final HTable hMetaTable,
            final long maxTimeToWaitForRegionMove) {
        this.admin = admin;
        this.hMetaTable = hMetaTable;
        this.maxTimeToWaitForRegionMove = maxTimeToWaitForRegionMove;
    }

    public boolean move(RegionPlan rp) throws Exception {
        admin.move(rp.getRegionInfo().getEncodedNameAsBytes(),
                Bytes.toBytes(rp.getDestination().getServerName()));

        return hasRegionMovedToDestination(hMetaTable, rp);
    }

    /**
     * Finds out whether the region is moved to the destination specified in the region plan.
     * 
     * @param hMetaTable
     * @param rp
     * @return false indicates different destination than the expected destination, true indicates same destination as expected destination.
     * @throws Exception
     */
    private boolean hasRegionMovedToDestination(HTable hMetaTable, RegionPlan rp)
            throws Exception {
        long startTime = System.currentTimeMillis();
        while (true) {
            String currHostingServerName = getServerNameForNonSystemRegion(
                    hMetaTable, rp.getRegionInfo());
            if (!rp.getSource().getServerName().equals(currHostingServerName)) {
                if (rp.getDestination().getServerName()
                        .equals(currHostingServerName))
                    return true;
                else
                    return false;
            }
            if (System.currentTimeMillis() - startTime > maxTimeToWaitForRegionMove)
                throw new Exception("Region check timed out : " + rp);
            Thread.sleep(3000);
        }
    }

    /**
     * Returns the server name for non meta and non system region.
     * 
     * @param table
     * @param hri
     * @return
     * @throws IOException
     */
    private static String getServerNameForNonSystemRegion(HTable table,
            HRegionInfo hri) throws IOException {
        if (hri.isMetaRegion() || hri.isRootRegion())
            throw new IllegalArgumentException(
                    "Given region is root or meta. Function does not implement servername for those kind of regions.");
        Get get = new Get(hri.getRegionName());
        get.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
        get.addColumn(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
        Result result = table.get(get);
        String serverName = Bytes.toString(result.getValue(
                HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER));
        long startCode = Bytes.toLong(result.getValue(
                HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER));
        return serverName.replaceFirst(":", ",") + "," + startCode;
    }
}
