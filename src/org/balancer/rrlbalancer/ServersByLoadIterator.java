package org.balancer.rrlbalancer;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Returns the {@link ServerAndLoadForTest} which have a load less than the given filtersize.
 * 
 */
@NotThreadSafe
public class ServersByLoadIterator implements
        Iterator<ServerAndAllClusteredRegions> {

    private final Iterator<ServerAndAllClusteredRegions> intItr;
    private final int filterSize;
    private final Queue<ServerAndAllClusteredRegions> processingQue;

    public ServersByLoadIterator(
            final Iterator<ServerAndAllClusteredRegions> intItr,
            final int filterSize) {
        this.intItr = intItr;
        this.filterSize = filterSize;
        processingQue = new ArrayDeque<ServerAndAllClusteredRegions>();
    }

    @Override
    public boolean hasNext() {
        if (processingQue.isEmpty())
            addElement();
        return processingQue.size() > 0;
    }

    @Override
    public ServerAndAllClusteredRegions next() {
        if (processingQue.size() > 0)
            return processingQue.remove();
        throw new NoSuchElementException(
                "No elements available to be returned.");
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Operation is not supported.");
    }

    private void addElement() {
        while (intItr.hasNext()) {
            ServerAndAllClusteredRegions temp = intItr.next();
            if (temp.getLoad() < filterSize) {
                processingQue.add(temp);
                break;
            }
        }
    }

}
