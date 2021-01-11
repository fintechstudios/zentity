package io.zentity.common;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CollectionUtil {
    /**
     * Partition a collection into sub-lists of a max size.
     *
     * @see <a href="https://stackoverflow.com/questions/12026885/is-there-a-common-java-utility-to-break-a-list-into-batches/58023258#58023258"></a>
     * @param input     the collection to partition.
     * @param chunkSize the max size of each sub-collection.
     * @return a collection of sub-lists.
     */
    public static <T> Collection<List<T>> partition(Collection<T> input, int chunkSize) {
        AtomicInteger counter = new AtomicInteger();
        return input.stream().collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize)).values();
    }
}
