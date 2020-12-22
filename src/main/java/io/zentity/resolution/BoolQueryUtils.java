package io.zentity.resolution;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class BoolQueryUtils {
    /**
     * All the possible ways a {@link BoolQueryBuilder} can combine queries.
     */
    public enum BoolQueryCombiner {
        FILTER,
        SHOULD,
        MUST,
        MUST_NOT
    }

    private static final Map<BoolQueryCombiner, BiFunction<BoolQueryBuilder, QueryBuilder, BoolQueryBuilder>> COMBINER_BI_FUNCTION_MAP;

    static {
        COMBINER_BI_FUNCTION_MAP = new HashMap<>();
        COMBINER_BI_FUNCTION_MAP.put(BoolQueryCombiner.FILTER, BoolQueryBuilder::filter);
        COMBINER_BI_FUNCTION_MAP.put(BoolQueryCombiner.SHOULD, BoolQueryBuilder::should);
        COMBINER_BI_FUNCTION_MAP.put(BoolQueryCombiner.MUST, BoolQueryBuilder::must);
        COMBINER_BI_FUNCTION_MAP.put(BoolQueryCombiner.MUST_NOT, BoolQueryBuilder::mustNot);
    }

    /**
     * Add all of one {@link BoolQueryBuilder}'s clauses into another.
     *
     * @param base The base query to add into.
     * @param toAdd The query to add from.
     * @return The base bool query with all of the second's clauses added.
     */
    private static BoolQueryBuilder addClauses(BoolQueryBuilder base, BoolQueryBuilder toAdd) {
        toAdd.filter().forEach(base::filter);
        toAdd.should().forEach(base::should);
        toAdd.must().forEach(base::must);
        toAdd.mustNot().forEach(base::mustNot);
        return base;
    }

    /**
     * Combine a stream of queries into one {@link BoolQueryBuilder}.
     *
     * @param combiner How the queries should be combined.
     * @param queries The queries to combine. Null items will be filtered out.
     * @return The combined query.
     */
    public static BoolQueryBuilder combineQueries(BoolQueryCombiner combiner, Stream<QueryBuilder> queries) {
        BiFunction<BoolQueryBuilder, QueryBuilder, BoolQueryBuilder> combineFunc = Optional
            .ofNullable(COMBINER_BI_FUNCTION_MAP.get(combiner))
            .orElseThrow(() -> new IllegalArgumentException("Invalid combiner: " + combiner));

        return queries
            .filter(Objects::nonNull)
            .reduce(
                QueryBuilders.boolQuery(),
                combineFunc,
                BoolQueryUtils::addClauses
            );
    }

    /**
     * Combine an array of queries into one {@link BoolQueryBuilder}.
     *
     * @param combiner How the queries should be combined.
     * @param queries The queries to combine. Null items will be filtered out.
     * @return The combined query.
     */
    public static BoolQueryBuilder combineQueries(BoolQueryCombiner combiner, QueryBuilder... queries) {
        return combineQueries(combiner, Arrays.stream(queries));
    }

    /**
     * Combine a collection of queries into one {@link BoolQueryBuilder}.
     *
     * @param combiner How the queries should be combined.
     * @param queries The queries to combine. Null items will be filtered out.
     * @return The combined query.
     */
    public static BoolQueryBuilder combineQueries(BoolQueryCombiner combiner, Collection<QueryBuilder> queries) {
        return combineQueries(combiner, queries.stream());
    }
}
