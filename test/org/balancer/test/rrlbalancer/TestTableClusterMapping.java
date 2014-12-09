package org.balancer.test.rrlbalancer;

import java.util.HashSet;
import java.util.Set;

import org.balancer.rrlbalancer.TableClusterMapping;
import org.junit.Assert;
import org.junit.Test;

public class TestTableClusterMapping {

    @Test
    public void testForCorrectMapping() {
        Set<String> relatedTables = new HashSet<String>();
        relatedTables.add("first");
        relatedTables.add("second");

        TableClusterMapping tcMapping = new TableClusterMapping();
        for (String relatedTable : relatedTables)
            Assert.assertFalse(tcMapping.isPartOfAnyCluster(relatedTable));
        tcMapping.addCluster(relatedTables);

        for (String relatedTable : relatedTables) {
            Assert.assertTrue(tcMapping.isPartOfAnyCluster(relatedTable));
            Assert.assertNotNull(tcMapping.getClusterName(relatedTable));
        }

        Assert.assertNotNull(tcMapping.getClusterName("third"));
    }

}
