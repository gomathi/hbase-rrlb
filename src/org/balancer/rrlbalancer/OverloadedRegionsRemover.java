package org.balancer.rrlbalancer;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

/**
 * Given a sorted iterator of {@link ServerAndLoadForTest}, it truncates the regions in each region server, if it has > maxSize regions.
 * 
 * When this iterator is exhausted by the caller, all the servers will have regions of 'maxSize'.
 */

@NotThreadSafe
public class OverloadedRegionsRemover implements
        Iterator<OverloadedRegionsRemover.TruncatedElement> {

    private final int maxSize;
    private final Iterator<ServerAndAllClusteredRegions> sortedServersItr;
    private final Queue<TruncatedElement> processingQue;
    private ServerAndAllClusteredRegions currProcessingServer;

    public OverloadedRegionsRemover(
            Iterator<ServerAndAllClusteredRegions> sortedServersItr,
            final int maxSize) {
        this.maxSize = maxSize;
        this.sortedServersItr = sortedServersItr;
        processingQue = new ArrayDeque<TruncatedElement>();
    }

    private void addNextEle() {
        if (currProcessingServer != null
                && currProcessingServer.getLoad() > maxSize) {
            processingQue
                    .add(new TruncatedElement(currProcessingServer
                            .removeNextCluster(), currProcessingServer
                            .getServerName()));
            return;
        }
        while (sortedServersItr.hasNext() && processingQue.isEmpty()) {
            currProcessingServer = sortedServersItr.next();
            if (currProcessingServer.getLoad() > maxSize) {
                processingQue.add(new TruncatedElement(currProcessingServer
                        .removeNextCluster(), currProcessingServer
                        .getServerName()));
                return;
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (processingQue.isEmpty())
            addNextEle();
        return processingQue.size() > 0;
    }

    @Override
    public TruncatedElement next() {
        if (processingQue.isEmpty())
            throw new NoSuchElementException();
        return processingQue.remove();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(
                "This operation is not supported.");
    }

    public static class TruncatedElement {
        public final List<HRegionInfo> regionsCluster;
        public final ServerName serverName;

        public TruncatedElement(
                List<HRegionInfo> regionsCluster, ServerName serverName) {
            this.regionsCluster = regionsCluster;
            this.serverName = serverName;
        }
    }
}
