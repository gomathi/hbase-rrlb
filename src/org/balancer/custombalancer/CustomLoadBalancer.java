package org.balancer.custombalancer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CustomLoadBalancer extends RelatedRegionsLoadBalancer {

    public static final List<Set<String>> relatedTablesList = new ArrayList<Set<String>>();
    public static final String CONF_FILE_DIR = "/tmp/hostweights";
    public static final String HOST_WEIGHTS_CONF_FILE = "hostweights.conf";

    static {
        Set<String> relatedTables = new HashSet<String>();
        relatedTablesList.add(relatedTables);
    }

    public CustomLoadBalancer() {
        super(relatedTablesList, CONF_FILE_DIR, HOST_WEIGHTS_CONF_FILE);
    }

    public CustomLoadBalancer(
            boolean balanceInBg) {
        super(relatedTablesList, CONF_FILE_DIR, HOST_WEIGHTS_CONF_FILE,
                balanceInBg);
    }
}
