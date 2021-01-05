package io.zentity.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FunctionalUtilTest {
    @Test
    public void testRecursiveFunction() {
        FunctionalUtil.Recursable<Integer, Integer> fact = (x, f) -> x == 0 ? 1 : x * f.apply(x - 1);

        assertEquals(Integer.valueOf(24), fact.apply(4));
    }
}
