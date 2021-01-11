package io.zentity.common;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class CompletableFutureUtilTest {
    @Test
    public void testRunParallel() throws ExecutionException, InterruptedException {
        List<Integer> seeds = List.of(0, 1, 2, 3, 4);

        List<Supplier<CompletableFuture<Integer>>> suppliers = seeds
            .stream()
            .map((i) -> (Supplier<CompletableFuture<Integer>>) () -> CompletableFuture.completedFuture(i))
            .collect(Collectors.toList());

        CompletableFuture<List<Integer>> resultFut = CompletableFutureUtil.runParallel(suppliers, 2);

        List<Integer> results = resultFut.get();

        assertEquals(seeds, results);
    }

    @Test
    public void testRunParallel1() throws ExecutionException, InterruptedException {
        List<Integer> seeds = List.of(0, 1, 2, 3, 4);

        List<Supplier<CompletableFuture<Integer>>> suppliers = seeds
            .stream()
            .map((i) -> (Supplier<CompletableFuture<Integer>>) () -> CompletableFuture.completedFuture(i))
            .collect(Collectors.toList());

        CompletableFuture<List<Integer>> resultFut = CompletableFutureUtil.runParallel(suppliers, 1);

        List<Integer> results = resultFut.get();

        assertEquals(seeds, results);
    }
}
