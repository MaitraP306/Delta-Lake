package com.delta.deltalake.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DeltaCacheVerificationTest {

    @Test
    void rejectsInvalidCapacity() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DeltaCache<String, String>(0)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new DeltaCache<String, String>(-1)
        );
    }

    @Test
    void supportsBasicPutGetContainsRemoveAndClear() {
        DeltaCache<String, String> cache = new DeltaCache<>(3);

        assertEquals(0, cache.size());
        assertFalse(cache.containsKey("a"));
        assertNull(cache.get("a"));

        cache.put("a", "A");
        cache.put("b", "B");

        assertEquals(2, cache.size());
        assertTrue(cache.containsKey("a"));
        assertTrue(cache.containsKey("b"));
        assertEquals("A", cache.get("a"));
        assertEquals("B", cache.get("b"));

        cache.remove("a");

        assertEquals(1, cache.size());
        assertFalse(cache.containsKey("a"));
        assertNull(cache.get("a"));
        assertTrue(cache.containsKey("b"));

        cache.clear();

        assertEquals(0, cache.size());
        assertFalse(cache.containsKey("b"));
        assertNull(cache.get("b"));
    }

    @Test
    void rejectsNullKeyAndNullValue() {
        DeltaCache<String, String> cache = new DeltaCache<>(2);

        assertThrows(
                NullPointerException.class,
                () -> cache.put(null, "value")
        );

        assertThrows(
                NullPointerException.class,
                () -> cache.put("key", null)
        );

        // Failed puts must not have inserted anything.
        assertEquals(0, cache.size());
        assertFalse(cache.containsKey("key"));
    }

    @Test
    void evictsLeastRecentlyUsedEntry() {
        DeltaCache<String, String> cache = new DeltaCache<>(2);

        cache.put("a", "A");
        cache.put("b", "B");

        assertEquals(2, cache.size());

        // Access "a", making it the most recently used entry.
        assertEquals("A", cache.get("a"));

        // "b" is now the least recently used entry.
        cache.put("c", "C");

        assertEquals(2, cache.size());

        assertTrue(cache.containsKey("a"));
        assertFalse(cache.containsKey("b"));
        assertTrue(cache.containsKey("c"));

        assertEquals("A", cache.get("a"));
        assertEquals("C", cache.get("c"));
    }

    @Test
    void evictsOldestEntryWhenNothingHasBeenAccessed() {
        DeltaCache<String, String> cache = new DeltaCache<>(3);

        cache.put("a", "A");
        cache.put("b", "B");
        cache.put("c", "C");

        cache.put("d", "D");

        assertEquals(3, cache.size());

        assertFalse(cache.containsKey("a"));
        assertTrue(cache.containsKey("b"));
        assertTrue(cache.containsKey("c"));
        assertTrue(cache.containsKey("d"));
    }

    @Test
    void getUpdatesLruOrder() {
        DeltaCache<Integer, String> cache = new DeltaCache<>(3);

        cache.put(1, "one");
        cache.put(2, "two");
        cache.put(3, "three");

        // 1 becomes most recently used.
        assertEquals("one", cache.get(1));

        // Current LRU order should effectively be:
        // 2 (oldest), 3, 1 (newest)
        cache.put(4, "four");

        assertFalse(cache.containsKey(2));
        assertTrue(cache.containsKey(1));
        assertTrue(cache.containsKey(3));
        assertTrue(cache.containsKey(4));
    }

    @Test
    void replacingExistingKeyDoesNotIncreaseSize() {
        DeltaCache<String, String> cache = new DeltaCache<>(2);

        cache.put("a", "old");
        cache.put("b", "B");

        assertEquals(2, cache.size());

        cache.put("a", "new");

        assertEquals(2, cache.size());
        assertEquals("new", cache.get("a"));
        assertEquals("B", cache.get("b"));
    }

    @Test
    void replacingExistingKeyMakesItRecentlyUsed() {
        DeltaCache<String, String> cache = new DeltaCache<>(2);

        cache.put("a", "A");
        cache.put("b", "B");

        // Replacing "a" counts as an access/update to the existing
        // entry in LinkedHashMap access-order mode.
        cache.put("a", "A2");

        cache.put("c", "C");

        assertTrue(cache.containsKey("a"));
        assertFalse(cache.containsKey("b"));
        assertTrue(cache.containsKey("c"));

        assertEquals("A2", cache.get("a"));
        assertEquals("C", cache.get("c"));
    }

    @Test
    void removingMissingKeyIsSafe() {
        DeltaCache<String, String> cache = new DeltaCache<>(2);

        cache.put("a", "A");

        cache.remove("does-not-exist");

        assertEquals(1, cache.size());
        assertEquals("A", cache.get("a"));
    }

    @Test
    void clearAllowsCacheToBeReused() {
        DeltaCache<String, String> cache = new DeltaCache<>(2);

        cache.put("a", "A");
        cache.put("b", "B");

        cache.clear();

        assertEquals(0, cache.size());

        cache.put("c", "C");

        assertEquals(1, cache.size());
        assertEquals("C", cache.get("c"));
        assertFalse(cache.containsKey("a"));
        assertFalse(cache.containsKey("b"));
    }

    @Test
    void capacityIsNeverExceeded() {
        int capacity = 5;
        DeltaCache<Integer, Integer> cache = new DeltaCache<>(capacity);

        for (int i = 0; i < 100; i++) {
            cache.put(i, i);

            assertTrue(
                    cache.size() <= capacity,
                    "Cache exceeded capacity after inserting key " + i
            );
        }

        assertEquals(capacity, cache.size());
    }
}