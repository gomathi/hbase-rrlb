package org.balancer.rrlbalancer;

import org.apache.hadoop.hbase.master.RegionPlan;

/**
 * Just used for providing mock implementations during unit testing.
 * 
 */
public interface RegionMoverInterface {

    /**
     * Moves region from source to destination as given in regionplan
     * 
     * @param rp
     * @return true indicates region moved to the destination specified in the region plan, false indicates region moved to different destination than the
     *         specified in the region plan.
     * @throws Exception
     *             , indicates there is a failure during the move of the region.
     */
    public boolean move(RegionPlan rp) throws Exception;

}
