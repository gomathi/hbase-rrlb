package org.balancer.rrlbalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

public class Utils {

    public static interface ClusterDataKeyGenerator<V, K> {
        K generateKey(V v);
    }

    /**
     * Complexity : In the average case O(n). Assuming that hash function evenly distributes all the region keys. In the worst case O(n ^ 2). It takes O(n)
     * storage in the worst case.
     * 
     * @param input
     * @param keyGenerator
     * @return
     */
    public static <K, V> Map<K, List<V>> cluster(Collection<V> input,
            ClusterDataKeyGenerator<V, K> keyGenerator) {
        Map<K, List<V>> result = new HashMap<K, List<V>>();
        for (V v : input) {
            K k = keyGenerator.generateKey(v);
            if (!result.containsKey(k))
                result.put(k, new ArrayList<V>());
            result.get(k).add(v);
        }

        return result;
    }

    public static <K, V> List<V> getMapEntriesForKeys(ListMultimap<K, V> map,
            Collection<K> keys) {
        List<V> result = new ArrayList<V>();
        for (K key : keys)
            if (map.containsKey(key))
                result.addAll(map.get(key));
        return result;
    }

    public static <K, V> Map<K, V> getMapEntriesForKeys(Map<K, V> map,
            Collection<K> keys) {
        Map<K, V> result = new HashMap<K, V>();
        for (K key : keys)
            if (map.containsKey(key))
                result.put(key, map.get(key));
        return result;
    }

    /**
     * Returns the common elements of a and b. Result is not a multi bag operation.
     * 
     * @param entriesA
     * @param entriesB
     * @return
     */
    public static <T> Set<T> intersect(Collection<T> entriesA,
            Collection<T> entriesB) {
        Set<T> cEntriesA = new TreeSet<T>();
        cEntriesA.addAll(entriesA);

        Set<T> result = new HashSet<T>();
        for (T b : entriesB) {
            if (cEntriesA.contains(b)) {
                result.add(b);
            }
        }
        return result;
    }

    /**
     * Returns the result of (a - b). Result is not multi bag operation. equivalent.
     * 
     * @param entriesA
     * @param entriesB
     * @return
     */
    public static <T> Set<T> minus(Collection<T> entriesA,
            Collection<T> entriesB) {
        Set<T> cEntriesB = new TreeSet<T>();
        cEntriesB.addAll(entriesB);

        Set<T> result = new HashSet<T>();
        for (T a : entriesA) {
            if (!cEntriesB.contains(a)) {
                result.add(a);
            }
        }
        return result;
    }

    public static <K, V> ListMultimap<V, K> reverseMap(Map<K, V> input) {
        ArrayListMultimap<V, K> multiMap = ArrayListMultimap.create();
        for (Map.Entry<K, V> entry : input.entrySet()) {
            multiMap.put(entry.getValue(), entry.getKey());
        }

        return multiMap;
    }

    public static <K, V> List<List<V>> getValuesAsList(Map<K, List<V>> input) {
        return new LinkedList<List<V>>(input.values());
    }

    public static <K, V> List<V> getValues(Map<K, V> input) {
        return new ArrayList<V>(input.values());
    }

    public static <K, V> void clearValues(Map<K, List<V>> input) {
        for (K key : input.keySet())
            input.get(key).clear();
    }

    public static Integer tryParse(String input) {
        Integer result = null;
        try {
            result = Integer.parseInt(input);
        }
        catch (NumberFormatException e) {
            // Intentionally ignored.
        }

        return result;
    }
}
