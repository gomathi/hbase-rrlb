package org.balancer.test.rrlbalancer;


import org.apache.hadoop.hbase.ServerName;

/**
 * Data structure that holds servername and 'load'.
 */
class TestServerAndLoad implements Comparable<TestServerAndLoad> {
    private final ServerName sn;
    private final int load;

    TestServerAndLoad(
            final ServerName sn, final int load) {
        this.sn = sn;
        this.load = load;
    }

    ServerName getServerName() {
        return this.sn;
    }

    int getLoad() {
        return this.load;
    }

    @Override
    public int compareTo(TestServerAndLoad other) {
        int diff = this.load - other.load;
        return diff != 0 ? diff : this.sn.compareTo(other.getServerName());
    }
}
