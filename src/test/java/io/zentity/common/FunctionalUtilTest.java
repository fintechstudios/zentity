package io.zentity.common;

import io.zentity.common.FunctionalUtil.Recursable;
import io.zentity.common.FunctionalUtil.UnCheckedFunction;
import org.elasticsearch.common.CheckedFunction;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FunctionalUtilTest {
    private static int throwsChecked(int i) throws IOException {
        throw new IOException(String.valueOf(i));
    }

    @Test
    public void testRecursiveFunction() {
        Recursable<Integer, Integer> fact = (x, f) -> x == 0 ? 1 : x * f.apply(x - 1);

        assertEquals(Integer.valueOf(24), fact.apply(4));
    }

    @Test
    public void testWrapCheckedFunction() {
        CheckedFunction<Integer, Integer, Exception> f = (i) -> { throw new Exception(String.valueOf(i)); };
        UnCheckedFunction<Integer, Integer, Exception> uf = UnCheckedFunction.from(f);

        // assertThrows w/ junit5
        try {
            uf.apply(1);
            fail("expected failure");
        } catch (Exception ex) {
            assertEquals("1", ex.getMessage());
        }
    }

    @Test
    public void testWrapCheckedDeclaredFunction() {
        UnCheckedFunction<Integer, Integer, Exception> uf = UnCheckedFunction.from(FunctionalUtilTest::throwsChecked);

        // assertThrows w/ junit5
        try {
            uf.apply(1);
            fail("expected failure");
        } catch (Exception ex) {
            assertEquals("1", ex.getMessage());
        }
    }
}
