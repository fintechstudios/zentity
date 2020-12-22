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

    private static BoolQueryBuilder addClauses(BoolQueryBuilder bool1, BoolQueryBuilder bool2) {
        bool2.filter().forEach(bool1::filter);
        bool2.should().forEach(bool1::should);
        bool2.must().forEach(bool1::must);
        bool2.mustNot().forEach(bool1::mustNot);
        return bool1;
    }

    public static BoolQueryBuilder fromQueries(BoolQueryCombiner combiner, Stream<QueryBuilder> builders) {
        BiFunction<BoolQueryBuilder, QueryBuilder, BoolQueryBuilder> combineFunc = Optional
            .ofNullable(COMBINER_BI_FUNCTION_MAP.get(combiner))
            .orElseThrow(() -> new IllegalArgumentException("Invalid combiner: " + combiner));

        return builders
            .filter(Objects::nonNull)
            .reduce(
                QueryBuilders.boolQuery(),
                combineFunc,
                BoolQueryUtils::addClauses
            );
    }

    public static BoolQueryBuilder fromQueries(BoolQueryCombiner combiner, QueryBuilder... builders) {
        return fromQueries(combiner, Arrays.stream(builders));
    }

    public static BoolQueryBuilder fromQueries(BoolQueryCombiner combiner, Collection<QueryBuilder> builders) {
        return fromQueries(combiner, builders.stream());
    }
}
