
package com.delta.deltalake.cache;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DeltaCacheTest {
    @Test
    void evictsLeastRecentlyUsedEntry() {
        DeltaCache<Integer, String> cache = new DeltaCache<>(2);
        cache.put(1, "a");
        cache.put(2, "b");
        assertEquals("a", cache.get(1));
        cache.put(3, "c");
        assertTrue(cache.containsKey(1));
        assertFalse(cache.containsKey(2));
        assertTrue(cache.containsKey(3));
    }
}
