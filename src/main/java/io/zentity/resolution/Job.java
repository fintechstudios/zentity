package io.zentity.resolution;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.zentity.common.ActionRequestUtil;
import io.zentity.common.CompletableFutureUtil;
import io.zentity.common.FunctionalUtil.UnCheckedBiFunction;
import io.zentity.common.FunctionalUtil.UnCheckedFunction;
import io.zentity.common.FunctionalUtil.UnCheckedSupplier;
import io.zentity.common.Json;
import io.zentity.common.Patterns;
import io.zentity.model.Index;
import io.zentity.model.IndexField;
import io.zentity.model.Matcher;
import io.zentity.model.Model;
import io.zentity.model.Resolver;
import io.zentity.model.ValidationException;
import io.zentity.resolution.BoolQueryUtils.BoolQueryCombiner;
import io.zentity.resolution.input.Attribute;
import io.zentity.resolution.input.Input;
import io.zentity.resolution.input.Term;
import io.zentity.resolution.input.value.Value;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchModule;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.zentity.resolution.BoolQueryUtils.BoolQueryCombiner.FILTER;
import static io.zentity.resolution.BoolQueryUtils.BoolQueryCombiner.SHOULD;

/**
 * A {@link Job} runs the resolution work.
 */
public class Job {

    // Constants
    public static final boolean DEFAULT_INCLUDE_ATTRIBUTES = true;
    public static final boolean DEFAULT_INCLUDE_ERROR_TRACE = true;
    public static final boolean DEFAULT_INCLUDE_EXPLANATION = false;
    public static final boolean DEFAULT_INCLUDE_HITS = true;
    public static final boolean DEFAULT_INCLUDE_QUERIES = false;
    public static final boolean DEFAULT_INCLUDE_SCORE = false;
    public static final boolean DEFAULT_INCLUDE_SEQ_NO_PRIMARY_TERM = false;
    public static final boolean DEFAULT_INCLUDE_SOURCE = true;
    public static final boolean DEFAULT_INCLUDE_VERSION = false;
    public static final int DEFAULT_MAX_DOCS_PER_QUERY = 1000;
    public static final int DEFAULT_MAX_HOPS = 100;
    public static final TimeValue DEFAULT_MAX_TIME_PER_QUERY = TimeValue.parseTimeValue("10s", "default_max_time_per_query");
    public static final boolean DEFAULT_PROFILE = false;

    // Job configuration
    private final NodeClient client;
    private final JobConfig config;

    // Job state
    private AttributeIdConfidenceScoreMap attributeIdConfidenceScores;
    private Map<String, Attribute> attributes;
    private Map<String, Set<String>> docIds;
    private List<JsonNode> hits;
    private List<LoggedQuery> queries;

    public Job(NodeClient client, JobConfig config) {
        this.client = client;
        this.config = config;
        initializeState();
    }

    private static Map<String, Collection<String>> buildResolverAttributeSummary(
        Map<String, Resolver> resolverMap,
        List<String> resolverNames
    ) {
        Map<String, Collection<String>> attributeSummary = new HashMap<>();
        for (String resolverName : resolverNames) {
            attributeSummary.put(resolverName, resolverMap.get(resolverName).attributes());
        }
        return attributeSummary;
    }

    private static LoggedQuery buildLoggedQuery(
        Input input,
        int hop,
        int queryNumber,
        String indexName,
        SearchRequestBuilder searchRequest,
        SearchResponse response,
        ElasticsearchException responseError,
        List<String> resolvers,
        Map<Integer, FilterTree> groupedResolversFilterTree,
        List<String> termResolvers,
        FilterTree termResolversFilterTree
    ) {
        // structure the filters
        Map<String, LoggedFilter> filters = new HashMap<>();

        LoggedFilter attrFilter = null;
        if (!resolvers.isEmpty() && !groupedResolversFilterTree.isEmpty()) {
            attrFilter = new LoggedFilter();
            attrFilter.resolverAttributes = buildResolverAttributeSummary(input.model().resolvers(), resolvers);
            attrFilter.groupedTree = groupedResolversFilterTree;
        }
        filters.put("attributes", attrFilter);

        LoggedFilter termsFilter = null;
        if (!resolvers.isEmpty() && !groupedResolversFilterTree.isEmpty()) {
            termsFilter = new LoggedFilter();
            termsFilter.resolverAttributes = buildResolverAttributeSummary(input.model().resolvers(), termResolvers);
            Map<Integer, FilterTree> groupedTermsFilterTree = new HashMap<>();
            groupedTermsFilterTree.put(0, termResolversFilterTree);
            termsFilter.groupedTree = groupedTermsFilterTree;
        }
        filters.put("terms", termsFilter);

        // structure the search data
        LoggedSearch search = new LoggedSearch();
        search.searchRequest = searchRequest;
        search.response = response;
        search.responseError = responseError;

        // put all the parts together
        LoggedQuery loggedQuery = new LoggedQuery();
        loggedQuery.search = search;
        loggedQuery.filters = filters;
        loggedQuery.index = indexName;
        loggedQuery.hop = hop;
        loggedQuery.queryNumber = queryNumber;

        return loggedQuery;
    }

    /**
     * Build a {@link Script} for index fields that are associated with "date" attributes.
     *
     * @param index
     * @param inputAttributes
     * @param model
     * @param attributeName
     * @param indexFieldName
     * @return
     * @throws ValidationException
     */
    public static Script buildDateAttributeScript(
        Index index,
        Map<String, Attribute> inputAttributes,
        Model model,
        String attributeName,
        String indexFieldName
    ) throws ValidationException {
        String format;
        // Check if the required params are defined in the input attribute.
        if (inputAttributes.containsKey(attributeName)
            && inputAttributes.get(attributeName).params().containsKey("format")
            && !inputAttributes.get(attributeName).params().get("format").equals("null")
            && !Patterns.EMPTY_STRING.matcher(inputAttributes.get(attributeName).params().get("format")).matches()) {
            format = inputAttributes.get(attributeName).params().get("format");
        } else {
            // Otherwise check if the required params are defined in the model attribute.
            Map<String, String> params = model.attributes().get(attributeName).params();
            if (params.containsKey("format")
                && !params.get("format").equals("null")
                && !Patterns.EMPTY_STRING.matcher(params.get("format")).matches()) {
                format = params.get("format");
            } else {
                // Otherwise check if the required params are defined in the matcher associated with the index field.
                String matcherName = index.attributeIndexFieldsMap().get(attributeName).get(indexFieldName).matcher();
                params = model.matchers().get(matcherName).params();
                if (params.containsKey("format")
                    && !params.get("format").equals("null")
                    && !Patterns.EMPTY_STRING.matcher(params.get("format")).matches()) {
                    format = params.get("format");
                } else {
                    // If we've gotten this far, that means that the required params for this attribute type
                    // haven't been specified in any valid places.
                    throw new ValidationException("'attributes." + attributeName + "' is a 'date' which required a 'format' to be specified in the params.");
                }
            }
        }

        // Make the "script" clause
        String scriptCode = "DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())";

        Map<String, Object> params = new HashMap<>();
        params.put("field", indexFieldName);
        params.put("format", format);

        return new Script(
            ScriptType.INLINE,
            "painless",
            scriptCode,
            params
        );
    }

    /**
     * Build the search "scripts" field.
     *
     * @param indexName The name of the index currently searching against.
     * @param input     The resolution input.
     * @return A map of script names => {@link Script Scripts} to be included in the search.
     * @throws ValidationException If the input is malformed.
     */
    public static Map<String, Script> buildScriptFields(String indexName, Input input) throws ValidationException {
        // Find any index fields that need to be included in the "script_fields" clause.
        // Currently this includes any index field that is associated with a "date" attribute,
        // which requires the "_source" value to be reformatted to a normalized format.
        Map<String, Script> scriptMap = new HashMap<>();

        Index index = input.model().indices().get(indexName);
        for (String attributeName : index.attributeIndexFieldsMap().keySet()) {
            switch (input.model().attributes().get(attributeName).type()) {
                case "date":
                    // Make a "script" clause for each index field associated with this attribute.
                    for (String indexFieldName : index.attributeIndexFieldsMap().get(attributeName).keySet()) {
                        Script script = buildDateAttributeScript(index, input.attributes(), input.model(), attributeName, indexFieldName);
                        scriptMap.put(indexFieldName, script);
                    }
                    break;

                default:
                    break;
            }
        }

        return scriptMap;
    }

    /**
     * Determine if a field of an index has a matcher associated with that field.
     *
     * @param model          The entity model.
     * @param indexName      The name of the index to reference in the entity model.
     * @param indexFieldName The name of the index field to reference in the index.
     * @return Boolean decision.
     */
    private static boolean indexFieldHasMatcher(Model model, String indexName, String indexFieldName) {
        String matcherName = model.indices().get(indexName).fields().get(indexFieldName).matcher();
        if (matcherName == null) {
            return false;
        }
        return model.matchers().get(matcherName) != null;
    }

    /**
     * Determine if we can construct a query for a given resolver on a given index with a given input.
     * Each attribute of the resolver must be mapped to a field of the index and have a matcher defined for it.
     *
     * @param model        The entity model.
     * @param indexName    The name of the index to reference in the entity model.
     * @param resolverName The name of the resolver to reference in the entity model.
     * @param attributes   The values for the input attributes.
     * @return Boolean decision.
     */
    private static boolean canQueryResolver(Model model, String indexName, String resolverName, Map<String, Attribute> attributes) {
        Map<String, Map<String, IndexField>> attributeIndexFieldMap = model.indices().get(indexName).attributeIndexFieldsMap();
        // Each attribute of the resolver must pass these conditions:
        for (String attributeName : model.resolvers().get(resolverName).attributes()) {

            // The input must have the attribute.
            if (!attributes.containsKey(attributeName)) {
                return false;
            }

            // The input must have at least one value for the attribute.
            if (attributes.get(attributeName).values().isEmpty()) {
                return false;
            }

            // The index must have at least one index field mapped to the attribute.
            if (!attributeIndexFieldMap.containsKey(attributeName)) {
                return false;
            }

            if (attributeIndexFieldMap.get(attributeName).isEmpty()) {
                return false;
            }

            // The index field must have a matcher defined for it.
            boolean hasMatcher = false;
            for (String indexFieldName : attributeIndexFieldMap.get(attributeName).keySet()) {
                if (indexFieldHasMatcher(model, indexName, indexFieldName)) {
                    hasMatcher = true;
                    break;
                }
            }
            if (!hasMatcher) {
                return false;
            }
        }
        return true;
    }

    /**
     * Given a clause from the "matchers" field of an entity model, replace the {{ field }} and {{ value }} variables
     * and arbitrary parameters. If a parameter exists, the value
     *
     * @param matcher        The matcher object.
     * @param indexFieldName The name of the index field to populate in the clause.
     * @param value          The value of the attribute to populate in the clause.
     * @param params         The values of the parameters (if any) to pass to the matcher.
     * @return A "bool" clause that references the desired field and value.
     */
    static QueryBuilder buildMatcherClause(
        Matcher matcher, String indexFieldName, String value, Map<String, String> params
    ) throws ValidationException, IOException {
        String matcherClause = matcher.clause();
        for (String variable : matcher.variables().keySet()) {
            Pattern pattern = matcher.variables().get(variable);
            switch (variable) {
                case "field":
                    matcherClause = pattern.matcher(matcherClause).replaceAll(indexFieldName);
                    break;
                case "value":
                    matcherClause = pattern.matcher(matcherClause).replaceAll(value);
                    break;
                default:
                    java.util.regex.Matcher m = Patterns.VARIABLE_PARAMS.matcher(variable);
                    if (m.find()) {
                        String var = m.group(1);
                        if (!params.containsKey(var)) {
                            throw new ValidationException("'matchers." + matcher.name() + "' was given no value for '{{ " + variable + " }}'");
                        }
                        String paramValue = params.get(var);
                        matcherClause = pattern.matcher(matcherClause).replaceAll(paramValue);
                    }
                    break;
            }
        }

        try (XContentParser parser = buildXContentParser(matcherClause)) {
            return AbstractQueryBuilder.parseInnerQueryBuilder(parser);
        }
    }

    static List<QueryBuilder> buildIndexFieldQueries(
        Model model,
        String indexName,
        Map<String, Attribute> attributes,
        String attributeName,
        BoolQueryCombiner combiner,
        boolean namedFilters,
        AtomicInteger nameIdCounter
    ) throws ValidationException, IOException {
        List<QueryBuilder> indexFieldQueries = new ArrayList<>();

        for (String indexFieldName : model.indices().get(indexName).attributeIndexFieldsMap().get(attributeName).keySet()) {
            // Can we use this index field?
            if (!indexFieldHasMatcher(model, indexName, indexFieldName)) {
                continue;
            }

            // Construct a clause for each input value for this attribute.
            String matcherName = model.indices().get(indexName).fields().get(indexFieldName).matcher();
            Matcher matcher = model.matchers().get(matcherName);
            List<QueryBuilder> valueClauses = new ArrayList<>();

            // Determine which values to pass to the matcher parameters.
            // Order of precedence:
            //  - Input attribute params override model attribute params
            //  - Model attribute params override matcher attribute params
            Map<String, String> params = new HashMap<>();
            params.putAll(matcher.params());
            params.putAll(model.attributes().get(attributeName).params());
            params.putAll(attributes.get(attributeName).params());

            Attribute attribute = attributes.get(attributeName);
            for (Value value : attribute.values()) {

                // Skip value if it's blank.
                if (value.serialized() == null || value.serialized().equals("")) {
                    continue;
                }

                // Populate the {{ field }}, {{ value }}, and {{ param.* }} variables of the matcher template.
                QueryBuilder valueClause = buildMatcherClause(matcher, indexFieldName, value.serialized(), params);
                if (namedFilters) {
                    // Name the clause to determine why any matching document matched
                    QueryValue queryValue = new QueryValue(
                        attributeName,
                        indexFieldName,
                        matcherName,
                        value,
                        nameIdCounter.getAndIncrement()
                    );
                    String name = queryValue.serialize();
                    valueClause = new BoolQueryBuilder()
                        .queryName(name)
                        .filter(valueClause);
                }
                valueClauses.add(valueClause);
            }
            if (valueClauses.size() == 0) {
                continue;
            }

            // Combine each value clause into a single "should" or "filter" clause.
            QueryBuilder valuesClause;
            if (valueClauses.size() > 1) {
                // build the combined bool query
                valuesClause = BoolQueryUtils.combineQueries(combiner, valueClauses);
            } else {
                valuesClause = valueClauses.get(0);
            }
            indexFieldQueries.add(valuesClause);
        }
        return indexFieldQueries;
    }

    /**
     * Given an entity model, an index name, and a set of attribute values,
     * for each attribute name in the set of attributes, find all index field names that are mapped to the attribute
     * name and populate their matcher clauses.
     *
     * @param model      The entity model.
     * @param indexName  The name of the index to reference in the entity model.
     * @param attributes The names and values of the input attributes.
     * @param combiner   Combine clauses with "should" or "filter".
     * @return The list of attribute clauses.
     */
    static List<QueryBuilder> buildAttributeQueries(
        Model model,
        String indexName,
        Map<String, Attribute> attributes,
        BoolQueryCombiner combiner,
        boolean namedFilters,
        AtomicInteger nameIdCounter
    ) throws ValidationException, IOException {
        List<QueryBuilder> attributeClauses = new ArrayList<>();
        for (String attributeName : attributes.keySet()) {

            // Construct a "should" or "filter" clause for each index field mapped to this attribute.
            List<QueryBuilder> indexFieldClauses = buildIndexFieldQueries(
                model,
                indexName,
                attributes,
                attributeName,
                combiner,
                namedFilters,
                nameIdCounter
            );
            if (indexFieldClauses.size() == 0) {
                continue;
            }

            // Combine each matcher clause into a single "should" or "filter" clause.
            QueryBuilder indexFieldsClause;
            if (indexFieldClauses.size() > 1) {
                indexFieldsClause = BoolQueryUtils.combineQueries(combiner, indexFieldClauses);
            } else {
                indexFieldsClause = indexFieldClauses.get(0);
            }
            attributeClauses.add(indexFieldsClause);
        }
        return attributeClauses;
    }

    static QueryBuilder buildResolversQuery(
        Model model,
        String indexName,
        FilterTree resolversFilterTree,
        Map<String, Attribute> attributes,
        boolean namedFilters, AtomicInteger nameIdCounter) throws ValidationException, IOException {
        // Construct a "filter" clause for each attribute at this level of the filter tree.
        List<QueryBuilder> clauses = new ArrayList<>();
        for (String attributeName : resolversFilterTree.keySet()) {

            // Construct a "should" clause for each index field mapped to this attribute.
            List<QueryBuilder> indexFieldClauses = buildIndexFieldQueries(
                model,
                indexName,
                attributes,
                attributeName,
                SHOULD,
                namedFilters,
                nameIdCounter
            );
            if (indexFieldClauses.size() == 0) {
                continue;
            }

            // Combine multiple matcher clauses into a single "should" clause.
            QueryBuilder indexFieldsClause;
            if (indexFieldClauses.size() > 1) {
                indexFieldsClause = BoolQueryUtils.combineQueries(SHOULD, indexFieldClauses);
            } else {
                indexFieldsClause = indexFieldClauses.get(0);
            }

            // Populate any child filters.
            QueryBuilder filter = buildResolversQuery(
                model,
                indexName,
                resolversFilterTree.get(attributeName),
                attributes,
                namedFilters,
                nameIdCounter
            );
            if (filter != null) {
                BoolQueryBuilder combo = BoolQueryUtils.combineQueries(FILTER, indexFieldsClause, filter);
                clauses.add(combo);
            } else {
                clauses.add(indexFieldsClause);
            }
        }

        // Combine each attribute clause into a single "should" clause.
        int size = clauses.size();
        if (size > 1) {
            return BoolQueryUtils.combineQueries(SHOULD, clauses);
        } else if (size == 1) {
            return clauses.get(0);
        }
        return null;
    }

    /**
     * Reorganize the attributes of all resolvers into a tree of Maps.
     *
     * @param resolversSorted The attributes for each resolver. Attributes are sorted first by priority and then lexicographically.
     * @param root            The root tree to add attributes into.
     * @return The attributes of all applicable resolvers nested in a tree.
     */
    static FilterTree makeResolversFilterTree(List<List<String>> resolversSorted, FilterTree root) {
        for (List<String> resolverSorted : resolversSorted) {
            FilterTree current = root;
            for (String attributeName : resolverSorted) {
                if (!current.containsKey(attributeName)) {
                    current.put(attributeName, new FilterTree());
                }
                current = current.get(attributeName);
            }
        }
        return root;
    }

    /**
     * Reorganize the attributes of all resolvers into a tree of Maps.
     *
     * @param resolversSorted The attributes for each resolver. Attributes are sorted first by priority and then lexicographically.
     * @return The attributes of all applicable resolvers nested in a tree.
     */
    static FilterTree makeResolversFilterTree(List<List<String>> resolversSorted) {
        return makeResolversFilterTree(resolversSorted, new FilterTree());
    }

    /**
     * Sort the attributes of each resolver in descending order by how many resolvers each attribute appears in,
     * and secondarily in ascending order by the name of the attribute.
     *
     * @param model     The entity model.
     * @param resolvers The names of the resolvers.
     * @param counts    For each attribute, the number of resolvers it appears in.
     * @return For each resolver, a list of attributes sorted first by priority and then lexicographically.
     */
    static List<List<String>> sortResolverAttributes(final Model model, final List<String> resolvers, final Map<String, Integer> counts) {
        return resolvers.stream()
            .map((resolverName) -> {
                Map<Integer, Set<String>> attributeGroups = new TreeMap<>();
                for (String attributeName : model.resolvers().get(resolverName).attributes()) {
                    int count = counts.get(attributeName);
                    if (!attributeGroups.containsKey(count)) {
                        attributeGroups.put(count, new TreeSet<>());
                    }
                    attributeGroups.get(count).add(attributeName);
                }

                Set<Integer> countsKeys = new TreeSet<>(Collections.reverseOrder());
                countsKeys.addAll(attributeGroups.keySet());

                List<String> resolverSorted = new ArrayList<>();
                for (int count : countsKeys) {
                    resolverSorted.addAll(attributeGroups.get(count));
                }
                return resolverSorted;
            })
            .collect(Collectors.toList());
    }

    /**
     * Count how many resolvers each attribute appears in.
     * Attributes that appear in more resolvers should be higher in the query tree.
     *
     * @param model     The entity model.
     * @param resolvers The names of the resolvers to reference in the entity model.
     * @return For each attribute, the number of resolvers it appears in.
     */
    static Map<String, Integer> countAttributesAcrossResolvers(Model model, List<String> resolvers) {
        Map<String, Integer> counts = new HashMap<>();
        for (String resolverName : resolvers) {
            for (String attributeName : model.resolvers().get(resolverName).attributes()) {
                counts.put(attributeName, counts.getOrDefault(attributeName, 0) + 1);
            }
        }
        return counts;
    }

    /**
     * Group resolvers by their level of weight.
     *
     * @param model     The entity model.
     * @param resolvers The names of the resolvers to reference in the entity model.
     * @return For each weight level, the names of the resolvers in that weight level.
     */
    public static Map<Integer, List<String>> groupResolversByWeight(Model model, List<String> resolvers) {
        Map<Integer, List<String>> resolverGroups = new TreeMap<>();
        for (String resolverName : resolvers) {
            Integer weight = model.resolvers().get(resolverName).weight();
            if (!resolverGroups.containsKey(weight)) {
                resolverGroups.put(weight, new ArrayList<>());
            }
            resolverGroups.get(weight).add(resolverName);
        }
        return resolverGroups;
    }

    /**
     * Combine a list of attribute identity confidence scores into a single composite identity confidence score using
     * conflation of probability distributions.
     * <p>
     * https://arxiv.org/pdf/0808.1808v4.pdf
     * <p>
     * If the list of attribute identity confidence scores contain both a 1.0 and a 0.0, this will lead to a division by
     * zero. When that happens, set the composite identity confidence score to 0.5, because there can be no certainty
     * when there are conflicting input scores that suggest both a complete confidence in a true match and a complete
     * confidence in a false match.
     *
     * @param attributeIdentityConfidenceScores
     */
    static Double calculateCompositeIdentityConfidenceScore(List<Double> attributeIdentityConfidenceScores) {
        Double compositeIdentityConfidenceScore = null;
        List<Double> scores = new ArrayList<>();
        List<Double> scoresInverse = new ArrayList<>();
        for (Double score : attributeIdentityConfidenceScores) {
            if (score != null) {
                scores.add(score);
                scoresInverse.add(1.0 - score);
            }
        }
        if (scores.size() > 0) {
            Double productScores = scores.stream().reduce(1.0, (a, b) -> a * b);
            Double productScoresInverse = scoresInverse.stream().reduce(1.0, (a, b) -> a * b);
            compositeIdentityConfidenceScore = productScores / (productScores + productScoresInverse);
            if (compositeIdentityConfidenceScore.isNaN()) {
                compositeIdentityConfidenceScore = 0.5;
            }
        }
        return compositeIdentityConfidenceScore;
    }

    /**
     * Calculate an attribute identity confidence score given a base score, a matcher quality score, and an index field
     * quality score. Any quality score of 0.0 will lead to a division by zero. When that happens, set the output score
     * to 0.0, because an attribute can give no confidence of an identity when any of the quality scores are 0.0.
     *
     * @param attributeIdentityConfidenceBaseScore
     * @param matcherQualityScore
     * @param indexFieldQualityScore
     * @return
     */
    static Double calculateAttributeIdentityConfidenceScore(Double attributeIdentityConfidenceBaseScore, Double matcherQualityScore, Double indexFieldQualityScore) {
        if (attributeIdentityConfidenceBaseScore == null) {
            return null;
        }
        double score = attributeIdentityConfidenceBaseScore;
        if (matcherQualityScore != null) {
            score = ((score - 0.5) / (score - 0.0) * ((score * matcherQualityScore) - score)) + score;
        }

        if (indexFieldQualityScore != null) {
            score = ((score - 0.5) / (score - 0.0) * ((score * indexFieldQualityScore) - score)) + score;
        }

        if (Double.isNaN(score)) {
            score = 0.0;
        }
        return score;
    }

    private static XContentParser buildXContentParser(String query) throws IOException {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        NamedXContentRegistry registry = new NamedXContentRegistry(searchModule.getNamedXContents());
        return XContentFactory.xContent(XContentType.JSON)
            .createParser(registry, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, query);
    }

    /**
     * Initializes/ resets the variables that hold the state of the job.
     */
    private void initializeState() {
        this.attributeIdConfidenceScores = new AttributeIdConfidenceScoreMap();
        this.attributes = new HashMap<>(this.config.input.attributes());
        this.docIds = new HashMap<>();
        this.hits = new ArrayList<>();
        this.queries = new ArrayList<>();
    }

    /**
     * Get a cached attribute identity confidence score, or calculate and cache an attribute identity confidence score.
     * This function helps minimize calculations over the life of the resolution job.
     *
     * @param attributeName
     * @param matcherName
     * @param indexName
     * @param indexFieldName
     * @return The confidence score.
     */
    private Double getAttributeIdentityConfidenceScore(String attributeName, String matcherName, String indexName, String indexFieldName) {
        // Return the cached match score if it exists.
        if (this.attributeIdConfidenceScores.hasScore(attributeName, matcherName, indexName, indexFieldName)) {
            return this.attributeIdConfidenceScores.getScore(attributeName, matcherName, indexName, indexFieldName);
        }

        Model inputModel = this.config.input.model();

        // Calculate the match score, cache it, and return it.
        Double attributeIdentityConfidenceBaseScore = inputModel.attributes().get(attributeName).score();
        if (attributeIdentityConfidenceBaseScore == null) {
            return null;
        }

        Double matcherQualityScore = inputModel.matchers().get(matcherName).quality();
        Double indexFieldQualityScore = inputModel.indices().get(indexName).fields().get(indexFieldName).quality();

        double score = calculateAttributeIdentityConfidenceScore(attributeIdentityConfidenceBaseScore, matcherQualityScore, indexFieldQualityScore);
        return this.attributeIdConfidenceScores.setScore(attributeName, matcherName, indexName, indexFieldName, score);
    }

    private boolean updateInputAttributes(Map<String, Attribute> nextInputAttributes) throws ValidationException {
        boolean newHits = false;
        for (String attributeName : nextInputAttributes.keySet()) {
            if (!this.attributes.containsKey(attributeName)) {
                String attributeType = this.config.input.model().attributes().get(attributeName).type();
                this.attributes.put(attributeName, new Attribute(attributeName, attributeType));
            }
            for (Value value : nextInputAttributes.get(attributeName).values()) {
                Set<Value> values = this.attributes.get(attributeName).values();
                if (!values.contains(value)) {
                    values.add(value);
                    newHits = true;
                }
            }
        }
        return newHits;
    }

    private Map<String, Set<Value>> buildTermValuesMap(String indexName, Set<String> resolverAttributes) {
        Map<String, Set<Value>> termValues = new HashMap<>();
        for (String attributeName : resolverAttributes) {
            String attributeType = this.config.input.model().attributes().get(attributeName).type();
            for (Term term : this.config.input.terms()) {
                try {
                    switch (attributeType) {
                        case "boolean":
                            if (term.isBoolean()) {
                                termValues.putIfAbsent(attributeName, new HashSet<>());
                                termValues.get(attributeName).add(term.booleanValue());
                            }
                            break;
                        case "date":
                            // Determine which date format to use to parse the term.
                            Index index = this.config.input.model().indices().get(indexName);
                            // Check if the "format" param is defined in the input attribute.
                            if (this.config.input.attributes().containsKey(attributeName)
                                && this.config.input.attributes().get(attributeName).params().containsKey("format")
                                && !this.config.input.attributes().get(attributeName).params().get("format").equals("null") && !Patterns.EMPTY_STRING.matcher(this.config.input.attributes().get(attributeName).params().get("format")).matches()) {
                                String format = this.config.input.attributes().get(attributeName).params().get("format");
                                if (term.isDate(format)) {
                                    termValues.putIfAbsent(attributeName, new HashSet<>());
                                    termValues.get(attributeName).add(term.dateValue());
                                }
                            } else {
                                // Otherwise check if the "format" param is defined in the model attribute.
                                Map<String, String> params = this.config.input.model().attributes().get(attributeName).params();
                                if (params.containsKey("format") && !params.get("format").equals("null") && !Patterns.EMPTY_STRING.matcher(params.get("format")).matches()) {
                                    String format = params.get("format");
                                    if (term.isDate(format)) {
                                        termValues.putIfAbsent(attributeName, new HashSet<>());
                                        termValues.get(attributeName).add(term.dateValue());
                                    }
                                } else {
                                    // Otherwise check if the "format" param is defined in the matcher
                                    // associated with any index field associated with the attribute.
                                    // Add any date values that successfully parse.
                                    for (String indexFieldName : index.attributeIndexFieldsMap().get(attributeName).keySet()) {
                                        String matcherName = index.attributeIndexFieldsMap().get(attributeName).get(indexFieldName).matcher();
                                        params = this.config.input.model().matchers().get(matcherName).params();
                                        if (params.containsKey("format") && !params.get("format").equals("null") && !Patterns.EMPTY_STRING.matcher(params.get("format")).matches()) {
                                            String format = params.get("format");
                                            if (term.isDate(format)) {
                                                termValues.putIfAbsent(attributeName, new HashSet<>());
                                                termValues.get(attributeName).add(term.dateValue());
                                            }
                                        }
                                        // else:
                                        // If we've gotten this far, then this term can't be converted
                                        // to a date value. Skip it and move on.
                                    }
                                }
                            }
                            break;
                        case "number":
                            if (term.isNumber()) {
                                termValues.putIfAbsent(attributeName, new HashSet<>());
                                termValues.get(attributeName).add(term.numberValue());
                            }
                            break;
                        case "string":
                            termValues.putIfAbsent(attributeName, new HashSet<>());
                            termValues.get(attributeName).add(term.stringValue());
                            break;
                        default:
                            break;
                    }
                } catch (ValidationException | IOException e) {
                    continue;
                }
            }
        }

        // Include any known attribute values in this clause.
        // This is necessary if a request has both "attributes" and "terms".
        if (!this.attributes.isEmpty()) {
            for (String attributeName : this.attributes.keySet()) {
                for (Value value : this.attributes.get(attributeName).values()) {
                    termValues.putIfAbsent(attributeName, new HashSet<>());
                    termValues.get(attributeName).add(value);
                }
            }
        }

        return termValues;
    }

    private Map<String, Attribute> buildAttributeMap(Map<String, Set<Value>> termValues) throws ValidationException {
        Map<String, Attribute> termAttributes = new HashMap<>();
        for (String attributeName : termValues.keySet()) {
            String attributeType = this.config.input.model().attributes().get(attributeName).type();
            Set<Value> values = termValues.get(attributeName);
            Map<String, Attribute> attributeMap = this.config.input.attributes();
            Map<String, String> params = attributeMap.containsKey(attributeName)
                ? attributeMap.get(attributeName).params()
                : Collections.emptyMap();
            termAttributes.put(attributeName, new Attribute(attributeName, attributeType, params, values));
        }
        return termAttributes;
    }

    private QueryBuilder buildSearchQuery(
        String indexName,
        boolean canQueryIds,
        boolean canQueryTerms,
        List<String> resolvers,
        AtomicInteger nameIdCounter,
        boolean namedFilters,
        Map<Integer, FilterTree> resolversFilterTreeGrouped,
        List<String> termResolvers,
        FilterTree termResolversFilterTree
    ) throws ValidationException, IOException {
        List<QueryBuilder> queryMustNotClauses = new ArrayList<>();
        List<QueryBuilder> queryFilterClauses = new ArrayList<>();

        // Exclude docs by _id
        Set<String> docIds = this.docIds.get(indexName);
        if (!docIds.isEmpty()) {
            queryMustNotClauses.add(new IdsQueryBuilder().addIds(docIds.toArray(new String[0])));
        }

        // Create "scope.exclude.attributes" clauses. Combine them into a single "should" clause.
        if (!this.config.input.scope().exclude().attributes().isEmpty()) {
            List<QueryBuilder> attributeClauses = buildAttributeQueries(
                this.config.input.model(),
                indexName,
                this.config.input.scope().exclude().attributes(),
                SHOULD,
                namedFilters,
                nameIdCounter
            );

            int size = attributeClauses.size();
            if (size > 1) {
                BoolQueryBuilder combo = BoolQueryUtils.combineQueries(SHOULD, attributeClauses);
                queryMustNotClauses.add(combo);
            } else if (size == 1) {
                queryMustNotClauses.add(attributeClauses.get(0));
            }
        }

        // Construct "scope.include.attributes" clauses. Combine them into a single "filter" clause.
        if (!this.config.input.scope().include().attributes().isEmpty()) {
            List<QueryBuilder> attributeClauses = buildAttributeQueries(
                this.config.input.model(),
                indexName,
                this.config.input.scope().include().attributes(),
                FILTER,
                namedFilters,
                nameIdCounter
            );
            int size = attributeClauses.size();
            if (size > 1) {
                BoolQueryBuilder combo = BoolQueryUtils.combineQueries(FILTER, attributeClauses);
                queryFilterClauses.add(combo);
            } else if (size == 1) {
                queryFilterClauses.add(attributeClauses.get(0));
            }
        }

        // Construct the "ids" clause if this is the first hop and if any ids are specified for this index.
        BoolQueryBuilder idsQuery = null;
        if (canQueryIds) {
            String[] ids = this.config.input.ids().get(indexName).toArray(new String[0]);
            idsQuery = QueryBuilders
                .boolQuery()
                .filter(QueryBuilders.idsQuery().addIds(ids));
        }

        // Construct the resolvers clause for attribute values.
        QueryBuilder resolversClause = null;
        FilterTree resolversFilterTree;

        if (!this.attributes.isEmpty()) {
            // Group the resolvers by their weight level.
            Map<Integer, List<String>> resolverGroups = groupResolversByWeight(this.config.input.model(), resolvers);

            // Construct a clause for each weight level in descending order of weight.
            List<Integer> weights = new ArrayList<>(resolverGroups.keySet());
            Collections.reverse(weights);
            int numWeightLevels = weights.size();
            for (int level = 0; level < numWeightLevels; level++) {
                Integer weight = weights.get(level);
                List<String> resolversGroup = resolverGroups.get(weight);
                Map<String, Integer> counts = countAttributesAcrossResolvers(this.config.input.model(), resolversGroup);
                List<List<String>> resolversSorted = sortResolverAttributes(this.config.input.model(), resolversGroup, counts);
                resolversFilterTree = makeResolversFilterTree(resolversSorted);
                resolversFilterTreeGrouped.put(numWeightLevels - level - 1, resolversFilterTree);
                resolversClause = buildResolversQuery(
                    this.config.input.model(),
                    indexName,
                    resolversFilterTree,
                    this.attributes,
                    namedFilters,
                    nameIdCounter
                );

                // If there are multiple levels of weight, then each lower weight group of resolvers must ensure
                // that every higher weight resolver either matches or does not exist.
                List<QueryBuilder> parentResolversClauses = new ArrayList<>();
                if (level > 0) {

                    // This is a lower weight group of resolvers.
                    // Every higher weight resolver either must match or must not exist.
                    for (int parentLevel = 0; parentLevel < level; parentLevel++) {
                        Integer parentWeight = weights.get(parentLevel);
                        List<String> parentResolversGroup = resolverGroups.get(parentWeight);
                        List<QueryBuilder> parentResolverClauses = new ArrayList<>();
                        for (String parentResolverName : parentResolversGroup) {

                            // Construct a clause that checks if any attribute of the resolver does not exist.
                            List<QueryBuilder> attributeExistsClauses = new ArrayList<>();
                            for (String attributeName : this.config.input.model().resolvers().get(parentResolverName).attributes()) {
                                BoolQueryBuilder notExistsClause = new BoolQueryBuilder();
                                notExistsClause.mustNot(new ExistsQueryBuilder(attributeName));
                                attributeExistsClauses.add(notExistsClause);
                            }
                            QueryBuilder attributesExistsClause = null;
                            if (attributeExistsClauses.size() > 1) {
                                attributesExistsClause = BoolQueryUtils.combineQueries(SHOULD, attributeExistsClauses);
                            } else if (attributeExistsClauses.size() == 1) {
                                attributesExistsClause = attributeExistsClauses.get(0);
                            }

                            // Construct a clause for the resolver.
                            List<String> parentResolverGroup = new ArrayList<>(Collections.singletonList(parentResolverName));
                            Map<String, Integer> parentCounts = countAttributesAcrossResolvers(this.config.input.model(), parentResolverGroup);
                            List<List<String>> parentResolverSorted = sortResolverAttributes(this.config.input.model(), parentResolverGroup, parentCounts);
                            FilterTree parentResolverFilterTree = makeResolversFilterTree(parentResolverSorted);
                            QueryBuilder parentResolverClause = buildResolversQuery(
                                this.config.input.model(),
                                indexName,
                                parentResolverFilterTree,
                                this.attributes,
                                namedFilters,
                                nameIdCounter
                            );

                            // Construct a "should" clause for the above two clauses.
                            BoolQueryBuilder combo = BoolQueryUtils.combineQueries(
                                SHOULD,
                                attributesExistsClause,
                                parentResolverClause
                            );

                            parentResolverClauses.add(combo);
                        }

                        if (parentResolverClauses.size() > 1) {
                            BoolQueryBuilder combo = BoolQueryUtils.combineQueries(FILTER, parentResolverClauses);
                            parentResolversClauses.add(combo);
                        } else if (parentResolverClauses.size() == 1) {
                            parentResolversClauses.add(parentResolverClauses.get(0));
                        }
                    }
                }

                // Combine the resolvers clause and parent resolvers clause in a "filter" query if necessary.
                if (parentResolversClauses.size() > 0) {
                    BoolQueryBuilder combo = BoolQueryUtils.combineQueries(FILTER, parentResolversClauses);

                    if (resolversClause != null) {
                        combo.filter(resolversClause);
                    }

                    resolversClause = combo;
                }
            }
        }

        // Construct the resolvers clause for any terms in the first hop.
        // Convert each term into each attribute value that matches its type.
        // Don't tier the resolvers by weights. Weights should be used only when the attribute values are certain.
        // In this case, terms are not certain to be attribute values of the entity until they match,
        // unlike structured attribute search where the attributes are assumed be known.
        if (canQueryTerms) {
            // Get the names of each attribute of each in-scope resolver.
            Set<String> resolverAttributes = new HashSet<>();
            for (String resolverName : this.config.input.model().resolvers().keySet()) {
                resolverAttributes.addAll(this.config.input.model().resolvers().get(resolverName).attributes());
            }

            // For each attribute, attempt to convert each term to a value of that attribute.
            // If the term does not match the attribute type, or if the term cannot be converted to a value
            // of that attribute, then skip the term and move on.
            //
            // Date attributes will require a format, but the format could be declared in the input attributes,
            // the model attributes, or the model matchers in descending order of precedence. If the pa
            Map<String, Set<Value>> termValues = buildTermValuesMap(indexName, resolverAttributes);

            // Convert the values as if it was an input Attribute.
            Map<String, Attribute> termAttributes = buildAttributeMap(termValues);

            // Determine which resolvers can be queried for this index using these attributes.
            for (String resolverName : this.config.input.model().resolvers().keySet()) {
                if (canQueryResolver(this.config.input.model(), indexName, resolverName, termAttributes)) {
                    termResolvers.add(resolverName);
                }
            }

            // Construct the resolvers clause for term attribute values.
            QueryBuilder termResolversClause = null;
            if (termResolvers.size() > 0) {
                Map<String, Integer> counts = countAttributesAcrossResolvers(this.config.input.model(), termResolvers);
                List<List<String>> termResolversSorted = sortResolverAttributes(this.config.input.model(), termResolvers, counts);
                termResolversFilterTree = makeResolversFilterTree(termResolversSorted, termResolversFilterTree);
                termResolversClause = buildResolversQuery(
                    this.config.input.model(),
                    indexName,
                    termResolversFilterTree,
                    termAttributes,
                    namedFilters,
                    nameIdCounter
                );
            }

            // Combine the two resolvers clauses in a "filter" clause if both exist.
            // If only the termResolversClause exists, set resolversClause to termResolversClause.
            // If neither clause exists, do nothing because resolversClause already does not exist.
            if (resolversClause != null && termResolversClause != null) {
                BoolQueryBuilder combo = BoolQueryUtils.combineQueries(FILTER, resolversClause, termResolversClause);
                queryFilterClauses.add(combo);
            } else if (termResolversClause != null) {
                resolversClause = termResolversClause;
            }
        }

        // Combine the ids clause and resolvers clause in a "should" clause if necessary.
        if (idsQuery != null && resolversClause != null) {
            BoolQueryBuilder combo = BoolQueryUtils.combineQueries(SHOULD, idsQuery, resolversClause);
            queryFilterClauses.add(combo);
        } else if (idsQuery != null) {
            queryFilterClauses.add(idsQuery);
        } else if (resolversClause != null) {
            queryFilterClauses.add(resolversClause);
        }

        // Construct the top-level "query" clause.
        QueryBuilder queryBuilder;
        if (!queryMustNotClauses.isEmpty() && !queryFilterClauses.isEmpty()) {
            // Construct the top-level "filter" clause. Combine this clause and the top-level "must_not" clause
            // in a "bool" clause and add it to the "query" field.
            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            queryMustNotClauses.forEach(boolQuery::mustNot);
            queryFilterClauses.forEach(boolQuery::filter);

            queryBuilder = boolQuery;
        } else if (!queryMustNotClauses.isEmpty()) {
            // Wrap only the top-level "must_not" clause in a "bool" clause and add it to the "query" field.
            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            queryMustNotClauses.forEach(boolQuery::mustNot);

            queryBuilder = boolQuery;
        } else if (!queryFilterClauses.isEmpty()) {
            // Construct the top-level "filter" clause and add only this clause to the "query" field.
            // This prevents a redundant "bool"."filter" wrapper clause when the top-level "must_not" clause
            // does not exist.
            if (queryFilterClauses.size() > 1) {
                BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                queryFilterClauses.forEach(boolQuery::filter);

                queryBuilder = boolQuery;
            } else {
                queryBuilder = queryFilterClauses.get(0);
            }
        } else {
            // This should never be reached.
            throw new IllegalStateException("No filter or mustNot clauses when building search query.");
        }

        return queryBuilder;
    }

    private void parseDocHitValue(
        Map<String, Attribute> nextInputAttributes,
        Map<String, Set<Value>> docAttributes,
        String attributeName,
        String attributeType,
        JsonNode valueNode
    ) throws ValidationException {
        Value value = Value.create(attributeType, valueNode);
        if (!docAttributes.containsKey(attributeName)) {
            docAttributes.put(attributeName, new HashSet<>());
        }
        if (!nextInputAttributes.containsKey(attributeName)) {
            nextInputAttributes.put(attributeName, new Attribute(attributeName, attributeType));
        }
        docAttributes.get(attributeName).add(value);
        nextInputAttributes.get(attributeName).values().add(value);
    }

    private void parseDocHitArrayValue(
        Map<String, Attribute> nextInputAttributes,
        Map<String, Set<Value>> docAttributes,
        String attributeName,
        String attributeType,
        JsonNode valueNode
    ) throws ValidationException {
        Iterator<JsonNode> valueNodeIterator = valueNode.elements();
        while (valueNodeIterator.hasNext()) {
            JsonNode vNode = valueNodeIterator.next();
            if (vNode.isNull() || valueNode.isMissingNode()) {
                continue;
            }
            parseDocHitValue(nextInputAttributes, docAttributes, attributeName, attributeType, vNode);
        }
    }

    private void parseDocHit(
        JsonNode doc,
        String indexName,
        Map<String, Attribute> nextInputAttributes,
        Map<String, Set<Value>> docAttributes,
        Map<String, JsonNode> docIndexFields
    ) throws ValidationException {
        for (String indexFieldName : this.config.input.model().indices().get(indexName).fields().keySet()) {
            String attributeName = this.config.input.model().indices().get(indexName).fields().get(indexFieldName).attribute();
            if (this.config.input.model().attributes().get(attributeName) == null)
                continue;
            String attributeType = this.config.input.model().attributes().get(attributeName).type();

            // Get the attribute values from the doc.
            if (doc.has("fields") && doc.get("fields").has(indexFieldName)) {
                // Get the attribute value from the "fields" field if it exists there.
                // This would include 'date' attribute types, for example.
                JsonNode valueNode = doc.get("fields").get(indexFieldName);
                if (valueNode.isNull() || valueNode.isMissingNode()) {
                    continue;
                } else if (valueNode.isArray()) {
                    parseDocHitArrayValue(nextInputAttributes, docAttributes, attributeName, attributeType, valueNode);
                    if (valueNode.size() == 1) {
                        docIndexFields.put(indexFieldName, valueNode.elements().next());
                    } else {
                        docIndexFields.put(indexFieldName, valueNode);
                    }
                } else {
                    parseDocHitValue(nextInputAttributes, docAttributes, attributeName, attributeType, valueNode);
                    docIndexFields.put(indexFieldName, valueNode);
                }
            } else {
                // TODO: these JsonPointers are the only thing holding us back from removing the manual JsonNode handling
                //       and just using SearchHits.
                // Get the attribute value from the "_source" field.
                // The index field name might not refer to the _source property.
                // If it's not in the _source, remove the last part of the index field name from the dot notation.
                // Index field names can reference multi-fields, which are not returned in the _source.
                // If the document does not contain a given index field, skip that field.
                IndexField indexField = this.config.input.model().indices().get(indexName).fields().get(indexFieldName);
                JsonPointer path = indexField.path();
                JsonPointer pathParent = indexField.pathParent();
                JsonNode valueNode = doc.get("_source").at(path);
                if (valueNode.isMissingNode()) {
                    if (pathParent != null) {
                        valueNode = doc.get("_source").at(pathParent);
                    } else {
                        continue;
                    }
                }
                if (valueNode.isNull() || valueNode.isMissingNode()) {
                    continue;
                }
                docIndexFields.put(indexFieldName, valueNode);
                if (valueNode.isArray()) {
                    parseDocHitArrayValue(nextInputAttributes, docAttributes, attributeName, attributeType, valueNode);
                } else {
                    parseDocHitValue(nextInputAttributes, docAttributes, attributeName, attributeType, valueNode);
                }
            }
        }
    }

    private void modifyDocMetadata(
        ObjectNode docObjNode,
        String indexName,
        int hop,
        int queryCount,
        boolean namedFilters,
        Map<String, Set<Value>> docAttributes,
        Map<String, JsonNode> docIndexFields
    ) throws IOException {
        docObjNode.remove("_score");
        docObjNode.remove("fields");
        docObjNode.put("_hop", hop);
        docObjNode.put("_query", queryCount);
        if (this.config.includeScore) {
            docObjNode.putNull("_score");
        }
        if (this.config.includeAttributes) {
            ObjectNode docAttributesObjNode = docObjNode.putObject("_attributes");
            for (String attributeName : docAttributes.keySet()) {
                ArrayNode docAttributeArrNode = docAttributesObjNode.putArray(attributeName);
                for (Value value : docAttributes.get(attributeName)) {
                    docAttributeArrNode.add(value.value());
                }
            }
        }

        // Determine why any matching documents matched if including "_score" or "_explanation".
        List<Double> bestAttributeIdentityConfidenceScores = new ArrayList<>();
        if (namedFilters && docObjNode.has("matched_queries") && docObjNode.get("matched_queries").size() > 0) {
            ObjectNode docExpObjNode = docObjNode.putObject("_explanation");
            ObjectNode docExpResolversObjNode = docExpObjNode.putObject("resolvers");
            ArrayNode docExpMatchesArrNode = docExpObjNode.putArray("matches");
            Set<String> expAttributes = new HashSet<>();
            Set<String> matchedQueryNames = new HashSet<>();

            // Create tuple-like objects that describe which attribute values matched which
            // index field values using which matchers and matcher parameters.
            Map<String, List<Double>> attributeIdConfidenceBaseScores = new HashMap<>();
            for (JsonNode mqNode : docObjNode.get("matched_queries")) {
                String serializedName = mqNode.asText();
                QueryValue queryValue = QueryValue.deserialize(serializedName);
                // skip duplicates
                if (!matchedQueryNames.add(queryValue.genericName())) {
                    continue;
                }
                ObjectNode docExpDetailsObjNode = Json.ORDERED_MAPPER.createObjectNode();

                String attributeName = queryValue.attributeName;
                String indexFieldName = queryValue.indexFieldName;
                String attributeValueSerialized = queryValue.serializedValue;
                String attributeType = this.config.input.model().attributes().get(attributeName).type();
                docExpDetailsObjNode.put("attribute", attributeName);
                docExpDetailsObjNode.put("target_field", indexFieldName);
                docExpDetailsObjNode.set("target_value", docIndexFields.get(indexFieldName));

                if (attributeType.equals("string") || attributeType.equals("date")) {
                    attributeValueSerialized = "\"" + attributeValueSerialized + "\"";
                }
                // TODO: manual json parsing + construction should be replaced
                JsonNode attributeValueNode = Json.MAPPER.readValue(attributeValueSerialized, JsonNode.class);
                docExpDetailsObjNode.set("input_value", attributeValueNode);

                String matcherName = queryValue.matcherName;
                String matcherParamsJson;
                if (this.config.input.attributes().containsKey(attributeName)) {
                    matcherParamsJson = Json.ORDERED_MAPPER.writeValueAsString(this.config.input.attributes().get(attributeName).params());
                } else if (this.config.input.model().matchers().containsKey(matcherName)) {
                    matcherParamsJson = Json.ORDERED_MAPPER.writeValueAsString(this.config.input.model().matchers().get(matcherName).params());
                } else {
                    matcherParamsJson = "{}";
                }
                JsonNode matcherParamsNode = Json.ORDERED_MAPPER.readTree(matcherParamsJson);
                docExpDetailsObjNode.put("input_matcher", matcherName);
                docExpDetailsObjNode.putPOJO("input_matcher_params", matcherParamsNode);

                // Calculate the attribute identity confidence score for this match.
                Double attributeIdentityConfidenceScore = null;
                if (this.config.includeScore) {
                    attributeIdentityConfidenceScore = this.getAttributeIdentityConfidenceScore(attributeName, matcherName, indexName, indexFieldName);
                    if (attributeIdentityConfidenceScore != null) {
                        attributeIdConfidenceBaseScores.putIfAbsent(attributeName, new ArrayList<>());
                        attributeIdConfidenceBaseScores.get(attributeName).add(attributeIdentityConfidenceScore);
                    }
                }

                if (this.config.includeScore) {
                    if (attributeIdentityConfidenceScore == null) {
                        docExpDetailsObjNode.putNull("score");
                    } else {
                        docExpDetailsObjNode.put("score", attributeIdentityConfidenceScore);
                    }
                }

                docExpMatchesArrNode.add(docExpDetailsObjNode);
                expAttributes.add(attributeName);
            }

            if (this.config.includeScore) {

                // Deconflict multiple attribute confidence scores for the same attribute
                // by selecting the highest score.
                for (String attributeName : attributeIdConfidenceBaseScores.keySet()) {
                    Double best = Collections.max(attributeIdConfidenceBaseScores.get(attributeName));
                    bestAttributeIdentityConfidenceScores.add(best);
                }

                // Combine the attribute confidence scores into a composite identity confidence score.
                Double documentConfidenceScore = calculateCompositeIdentityConfidenceScore(bestAttributeIdentityConfidenceScores);
                if (documentConfidenceScore != null) {
                    docObjNode.put("_score", documentConfidenceScore);
                }
            }

            // Summarize matched resolvers
            for (String resolverName : this.config.input.model().resolvers().keySet()) {
                if (expAttributes.containsAll(this.config.input.model().resolvers().get(resolverName).attributes())) {
                    ObjectNode docExpResolverObjNode = docExpResolversObjNode.putObject(resolverName);
                    ArrayNode docExpResolverAttributesArrNode = docExpResolverObjNode.putArray("attributes");
                    for (String attributeName : this.config.input.model().resolvers().get(resolverName).attributes()) {
                        docExpResolverAttributesArrNode.add(attributeName);
                    }
                }
            }
            docObjNode.remove("matched_queries");
            if (!this.config.includeExplanation) {
                docObjNode.remove("_explanation");
            }
        }

        // Either remove "_source" or move "_source" under "_attributes".
        if (!this.config.includeSource) {
            docObjNode.remove("_source");
        } else {
            // TODO: this doesn't nest _source under _attributes?
            JsonNode sourceNode = docObjNode.get("_source");
            docObjNode.remove("_source");
            docObjNode.set("_source", sourceNode);
        }

        // Store doc in response.
        this.hits.add(docObjNode);
    }

    private SearchRequestBuilder buildSearchRequest(String indexName) {
        final SearchRequestBuilder searchReqBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
        searchReqBuilder
            .setFetchSource(true)
            .setIndices(indexName)
            .setFetchSource(true)
            .setSize(this.config.maxDocsPerQuery)
            .setProfile(this.config.profile)
            .seqNoAndPrimaryTerm(this.config.includeSeqNoPrimaryTerm)
            .setVersion(this.config.includeVersion);

        if (this.config.searchAllowPartialSearchResults != null) {
            searchReqBuilder.setAllowPartialSearchResults(this.config.searchAllowPartialSearchResults);
        }
        if (this.config.searchBatchedReduceSize != null) {
            searchReqBuilder.setBatchedReduceSize(this.config.searchBatchedReduceSize);
        }
        if (this.config.searchMaxConcurrentShardRequests != null) {
            searchReqBuilder.setMaxConcurrentShardRequests(this.config.searchMaxConcurrentShardRequests);
        }
        if (this.config.searchPreFilterShardSize != null) {
            searchReqBuilder.setPreFilterShardSize(this.config.searchPreFilterShardSize);
        }
        if (this.config.searchPreference != null) {
            searchReqBuilder.setPreference(this.config.searchPreference);
        }
        if (this.config.searchRequestCache != null) {
            searchReqBuilder.setRequestCache(this.config.searchRequestCache);
        }
        if (this.config.maxTimePerQuery != null) {
            searchReqBuilder.setTimeout(this.config.maxTimePerQuery);
        }

        return searchReqBuilder;
    }

    /**
     * Given a set of attribute values, determine which queries to submit to which indices then submit them and recurse
     * asynchronously.
     */
    private CompletableFuture<ResolutionResponse> traverseAsync() {
        final AtomicBoolean newAttributeHits = new AtomicBoolean(true);
        final AtomicInteger hop = new AtomicInteger(0);
        final AtomicInteger maxHops = new AtomicInteger(this.config.maxHops <= -1 ? Integer.MAX_VALUE : this.config.maxHops);
        final AtomicBoolean namedFilters = new AtomicBoolean(this.config.includeExplanation || this.config.includeScore);
        final Set<String> missingIndices = Collections.synchronizedSet(new HashSet<>());
        final Map<String, Attribute> nextInputAttributes = Collections.synchronizedMap(new HashMap<>());
        final AtomicInteger queryCounter = new AtomicInteger(0);

        final CompletableFuture<Void> emptyResultFut = CompletableFuture.completedFuture(null);

        final Predicate<Void> shouldContinuePred = (nil) -> newAttributeHits.get() && !(hop.get() > maxHops.get());

        final CheckedFunction<String, CompletableFuture<Void>, IOException> runIndexSearch = (indexName) -> {
            // Skip this index if a prior hop determined the index to be missing.
            if (missingIndices.contains(indexName)) {
                return emptyResultFut;
            }

            // Track _ids for this index.
            if (!this.docIds.containsKey(indexName)) {
                this.docIds.put(indexName, new HashSet<>());
            }

            // "_explanation" uses named queries, and each value of the "_name" fields must be unique.
            // Use a counter to prepend a unique and deterministic identifier for each "_name" field in the query.
            AtomicInteger nameIdCounter = new AtomicInteger();

            // Determine which resolvers can be queried for this index.
            List<String> resolvers = new ArrayList<>();
            Set<String> resolverNames = this.config.input.model().resolvers().keySet();
            for (String resolverName : resolverNames) {
                if (canQueryResolver(this.config.input.model(), indexName, resolverName, this.attributes)) {
                    resolvers.add(resolverName);
                }
            }

            // Determine if we can query this index.
            boolean canQueryIds = hop.get() == 0
                && this.config.input.ids().containsKey(indexName)
                && !this.config.input.ids().get(indexName).isEmpty();

            boolean canQueryTerms = hop.get() == 0 &&
                !this.config.input.terms().isEmpty();

            if (resolvers.size() == 0 && !canQueryIds && !canQueryTerms) {
                return emptyResultFut;
            }

            final SearchRequestBuilder searchReqBuilder = buildSearchRequest(indexName);
            Map<String, Script> scripts = buildScriptFields(indexName, this.config.input);
            scripts.forEach(searchReqBuilder::addScriptField);

            final Map<Integer, FilterTree> resolversFilterTreeGrouped = new TreeMap<>(Collections.reverseOrder());
            // Construct query for this index.
            final List<String> termResolvers = new ArrayList<>();
            final FilterTree termResolversFilterTree = new FilterTree();

            final QueryBuilder searchQuery = buildSearchQuery(
                indexName,
                canQueryIds,
                canQueryTerms,
                resolvers,
                nameIdCounter,
                namedFilters.get(),
                resolversFilterTreeGrouped,
                termResolvers,
                termResolversFilterTree
            );
            searchReqBuilder.setQuery(searchQuery);

            // Submit query to Elasticsearch.
            return ActionRequestUtil
                .toCompletableFuture(searchReqBuilder)
                .handle(UnCheckedBiFunction.from((response, throwable) -> {
                    ElasticsearchException responseError = null;
                    boolean fatalError = false;

                    if (throwable != null) {
                        Throwable cause = CompletableFutureUtil.getCause(throwable);

                        if (cause instanceof IndexNotFoundException) {
                            IndexNotFoundException idxEx = (IndexNotFoundException) cause;
                            // Don't fail the job if an index was missing.
                            missingIndices.add(idxEx.getIndex().getName());
                            responseError = idxEx;
                        } else {
                            // TODO: maybe just throw this error?
                            fatalError = true;
                            responseError = (ElasticsearchException) cause;
                        }
                    }

                    // Log queries.
                    if (config.includeQueries || config.profile) {
                        LoggedQuery logged = buildLoggedQuery(
                            config.input,
                            hop.get(),
                            queryCounter.get(),
                            indexName,
                            searchReqBuilder,
                            response,
                            responseError,
                            resolvers,
                            resolversFilterTreeGrouped,
                            termResolvers,
                            termResolversFilterTree
                        );
                        queries.add(logged);
                    }

                    // Stop traversing if there was an error not due to a missing index.
                    if (fatalError) {
                        throw responseError;
                    }

                    // Read response from Elasticsearch.
                    JsonNode responseData = null;
                    if (response != null) {
                        responseData = Json.ORDERED_MAPPER.readTree(response.toString());
                    }

                    // Read the hits
                    if (responseData == null) {
                        return null;
                    }
                    if (!responseData.has("hits")) {
                        return null;
                    }
                    if (!responseData.get("hits").has("hits")) {
                        return null;
                    }

                    // TODO: don't parse response as JSON, use SearchHit from response.getHits().getHits()
                    for (JsonNode doc : responseData.get("hits").get("hits")) {
                        // Skip doc if already fetched. Otherwise mark doc as fetched and then proceed.
                        String id = doc.get("_id").textValue();
                        Set<String> indexDocIds = docIds.get(indexName);
                        if (indexDocIds.contains(id)) {
                            continue;
                        }
                        indexDocIds.add(id);

                        // Gather attributes from the doc. Store them in the "_attributes" field of the doc,
                        // and include them in the attributes for subsequent queries.
                        Map<String, Set<Value>> docAttributes = new HashMap<>();
                        Map<String, JsonNode> docIndexFields = new HashMap<>();

                        parseDocHit(doc, indexName, nextInputAttributes, docAttributes, docIndexFields);

                        // Modify doc metadata.
                        if (config.includeHits) {
                            modifyDocMetadata(
                                (ObjectNode) doc,
                                indexName,
                                hop.get(),
                                queryCounter.get(),
                                namedFilters.get(),
                                docAttributes,
                                docIndexFields
                            );
                        }
                    }
                    queryCounter.incrementAndGet();
                    return null;
                }));
        };

        final CheckedSupplier<CompletableFuture<Void>, IOException> runTraversal = () -> {
            nextInputAttributes.clear();
            queryCounter.set(0);

            /*
             * What's this loop doing?
             * For each of the model's indices:
             * - Track the ids
             * - find resolvers to use for the index
             * - construct a query for the index
             * - run the query
             * - log the query result
             * - deconstruct the response
             * - calculate explanations for hits
             *
             * Early exits:
             * - searching an index already marked as missing
             * - cannot query attributes, ids, or terms (terms and ids are only ever queried on first run)
             * - search response error
             * - search response doesn't have hits
             */

            // Construct a query for each index that maps to a resolver.
            Set<String> indices = this.config.input.model().indices().keySet();
            CompletableFuture<Void> completeFut = CompletableFuture.completedFuture(null);
            for (String indexName : indices) {
                completeFut = completeFut.thenCompose(UnCheckedFunction.from((res) -> runIndexSearch.apply(indexName)));
            }

            return completeFut
                .thenApply((nil) -> {
                    // Update input attributes for the next queries.
                    newAttributeHits.set(updateInputAttributes(nextInputAttributes));
                    // Update hop count.
                    hop.incrementAndGet();
                    return null;
                });
        };

        // Start timer and begin job
        final long startTime = System.nanoTime();

        Function<Void, CompletableFuture<Void>> traversalFunc = CompletableFutureUtil
            .recursiveLoopFunction(
                shouldContinuePred.negate(),
                UnCheckedSupplier.from(runTraversal)
            );

        return traversalFunc.apply(null)
            .handle((res, err) -> {
                // Format response
                ResolutionResponse response = new ResolutionResponse();
                response.took = Duration.ofNanos(System.nanoTime() - startTime);
                response.hits = this.hits;
                response.includeHits = this.config.includeHits;

                // TODO: maybe move these "includeX" settings to where the response is needed
                response.queries = this.queries;
                response.includeQueries = this.config.includeQueries || this.config.profile;
                response.error = err == null ? null : CompletableFutureUtil.getCause(err);
                response.includeStackTrace = this.config.includeErrorTrace;
                return response;
            });
    }

    /**
     * Run the entity resolution job. Not thread-safe.
     *
     * @return A JSON string to be returned as the body of the response to a client.
     */
    public CompletableFuture<ResolutionResponse> runAsync() {
        // initialize the state in case this was run before
        this.initializeState();
        return this.traverseAsync();
    }

    /**
     * Get a new instance of a {@link Builder}.
     *
     * @return The builder.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * All configuration options for the {@link Job}.
     */
    public static class JobConfig {
        private Input input;
        private boolean includeAttributes = DEFAULT_INCLUDE_ATTRIBUTES;
        private boolean includeErrorTrace = DEFAULT_INCLUDE_ERROR_TRACE;
        private boolean includeExplanation = DEFAULT_INCLUDE_EXPLANATION;
        private boolean includeHits = DEFAULT_INCLUDE_HITS;
        private boolean includeQueries = DEFAULT_INCLUDE_QUERIES;
        private boolean includeScore = DEFAULT_INCLUDE_SCORE;
        private boolean includeSeqNoPrimaryTerm = DEFAULT_INCLUDE_SEQ_NO_PRIMARY_TERM;
        private boolean includeSource = DEFAULT_INCLUDE_SOURCE;
        private boolean includeVersion = DEFAULT_INCLUDE_VERSION;
        private int maxDocsPerQuery = DEFAULT_MAX_DOCS_PER_QUERY;
        private int maxHops = DEFAULT_MAX_HOPS;
        private TimeValue maxTimePerQuery = DEFAULT_MAX_TIME_PER_QUERY;
        private boolean profile = DEFAULT_PROFILE;

        // optional, nullable search parameters
        private Boolean searchAllowPartialSearchResults = null;
        private Integer searchBatchedReduceSize = null;
        private Integer searchMaxConcurrentShardRequests = null;
        private Integer searchPreFilterShardSize = null;
        private String searchPreference = null;
        private Boolean searchRequestCache = null;
    }

    /**
     * A builder for a {@link Job}.
     */
    public static class Builder {
        private NodeClient client;
        private final JobConfig config = new JobConfig();

        public Builder includeAttributes(boolean includeAttributes) {
            this.config.includeAttributes = includeAttributes;
            return this;
        }

        public Builder includeErrorTrace(boolean includeErrorTrace) {
            this.config.includeErrorTrace = includeErrorTrace;
            return this;
        }

        public Builder includeExplanation(boolean includeExplanation) {
            this.config.includeExplanation = includeExplanation;
            return this;
        }

        public Builder includeHits(boolean includeHits) {
            this.config.includeHits = includeHits;
            return this;
        }

        public Builder includeQueries(boolean includeQueries) {
            this.config.includeQueries = includeQueries;
            return this;
        }

        public Builder includeScore(Boolean includeScore) {
            this.config.includeScore = includeScore;
            return this;
        }

        public Builder includeSeqNoPrimaryTerm(Boolean includeSeqNoPrimaryTerm) {
            this.config.includeSeqNoPrimaryTerm = includeSeqNoPrimaryTerm;
            return this;
        }

        public Builder includeSource(boolean includeSource) {
            this.config.includeSource = includeSource;
            return this;
        }

        public Builder includeVersion(Boolean includeVersion) {
            this.config.includeVersion = includeVersion;
            return this;
        }

        public Builder maxDocsPerQuery(int maxDocsPerQuery) {
            this.config.maxDocsPerQuery = maxDocsPerQuery;
            return this;
        }

        public Builder maxHops(int maxHops) {
            this.config.maxHops = maxHops;
            return this;
        }

        public Builder maxTimePerQuery(TimeValue maxTimePerQuery) {
            if (maxTimePerQuery != null) {
                this.config.maxTimePerQuery = maxTimePerQuery;
            }
            return this;
        }

        public Builder profile(Boolean profile) {
            this.config.profile = profile;
            return this;
        }

        public Builder searchAllowPartialSearchResults(Boolean searchAllowPartialSearchResults) {
            this.config.searchAllowPartialSearchResults = searchAllowPartialSearchResults;
            return this;
        }

        public Builder searchBatchedReduceSize(Integer searchBatchedReduceSize) {
            this.config.searchBatchedReduceSize = searchBatchedReduceSize;
            return this;
        }

        public Builder searchMaxConcurrentShardRequests(Integer searchMaxConcurrentShardRequests) {
            this.config.searchMaxConcurrentShardRequests = searchMaxConcurrentShardRequests;
            return this;
        }

        public Builder searchPreFilterShardSize(Integer searchPreFilterShardSize) {
            this.config.searchPreFilterShardSize = searchPreFilterShardSize;
            return this;
        }

        public Builder searchPreference(String searchPreference) {
            this.config.searchPreference = searchPreference;
            return this;
        }

        public Builder searchRequestCache(Boolean searchRequestCache) {
            this.config.searchRequestCache = searchRequestCache;
            return this;
        }

        public Builder input(Input input) {
            this.config.input = input;
            return this;
        }

        public Builder client(NodeClient client) {
            this.client = client;
            return this;
        }

        public Job build() {
            if (this.client == null) {
                throw new IllegalStateException("Must set client");
            }
            return new Job(client, config);
        }
    }
}
