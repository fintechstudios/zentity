package io.zentity.resolution;

import io.zentity.model.Matcher;
import io.zentity.model.Model;
import io.zentity.model.ValidationException;
import io.zentity.resolution.input.Input;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.Script;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.zentity.resolution.BoolQueryUtils.BoolQueryCombiner.FILTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JobTest {
    @Test
    public void testMakeResolversClause() throws Exception {
        String attributes = "\"attributes\":{\"name\":{},\"street\":{},\"city\":{},\"state\":{},\"zip\":{},\"phone\":{},\"id\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"name\",\"street\",\"city\",\"state\"]},\"b\":{\"attributes\":[\"name\",\"street\",\"zip\"]},\"c\":{\"attributes\":[\"name\",\"phone\"]},\"d\":{\"attributes\":[\"id\"]}}";
        String matchers = "\"matchers\":{\"x\":{\"clause\":{\"term\":{\"{{field}}\":\"{{value}}\"}}},\"y\":{\"clause\":{\"match\":{\"{{field}}\":{\"query\":\"{{value}}\",\"fuzziness\":\"{{ params.fuzziness }}\"}}}},\"z\":{\"clause\":{\"match\":{\"{{field}}\":{\"query\":\"{{value}}\",\"fuzziness\":\"{{ params.fuzziness }}\"}}},\"params\":{\"fuzziness\":\"1\"}}}";
        String indices = "\"indices\":{\"index\":{\"fields\":{\"name\":{\"attribute\":\"name\",\"matcher\":\"x\"},\"street\":{\"attribute\":\"street\",\"matcher\":\"x\"},\"city\":{\"attribute\":\"city\",\"matcher\":\"x\"},\"state\":{\"attribute\":\"state\",\"matcher\":\"x\"},\"zip\":{\"attribute\":\"zip\",\"matcher\":\"x\"},\"phone\":{\"attribute\":\"phone\",\"matcher\":\"z\"},\"id\":{\"attribute\":\"id\",\"matcher\":\"y\"}}}}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {" +
            "    \"name\": { \"values\": [ \"Alice Jones\", \"Alice Jones-Smith\" ]}," +
            "    \"street\": { \"values\": [ \"123 Main St\" ]}," +
            "    \"city\": { \"values\": [ \"Beverly Hills\" ]}," +
            "    \"state\": { \"values\": [ \"CA\" ]}," +
            "    \"zip\": [ \"90210\" ]," +
            "    \"phone\": { \"values\": [ \"555-123-4567\" ], \"params\": { \"fuzziness\": \"2\" }}," +
            "    \"id\": { \"values\": [ \"1234567890\" ], \"params\": { \"fuzziness\": \"auto\" }}" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);
        List<String> resolversList = new ArrayList<>();
        resolversList.add("a");
        resolversList.add("b");
        resolversList.add("c");
        resolversList.add("d");
        Map<String, Integer> counts = Job.countAttributesAcrossResolvers(model, resolversList);
        List<List<String>> resolversSorted = Job.sortResolverAttributes(model, resolversList, counts);
        FilterTree resolversFilterTree = Job.makeResolversFilterTree(resolversSorted);
        QueryBuilder resolversQuery = Job.buildResolversQuery(
            model, "index", resolversFilterTree, input.attributes(), false, new AtomicInteger()
        );
        assertNotNull(resolversQuery);

        String expected = "{\"bool\":{\"should\":[{\"match\":{\"id\":{\"query\":\"1234567890\",\"operator\":\"OR\",\"fuzziness\":\"AUTO\",\"prefix_length\":0,\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\",\"auto_generate_synonyms_phrase_query\":true,\"boost\":1.0}}},{\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"term\":{\"name\":{\"value\":\"Alice Jones\",\"boost\":1.0}}},{\"term\":{\"name\":{\"value\":\"Alice Jones-Smith\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}},{\"bool\":{\"should\":[{\"match\":{\"phone\":{\"query\":\"555-123-4567\",\"operator\":\"OR\",\"fuzziness\":\"2\",\"prefix_length\":0,\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\",\"auto_generate_synonyms_phrase_query\":true,\"boost\":1.0}}},{\"bool\":{\"filter\":[{\"term\":{\"street\":{\"value\":\"123 Main St\",\"boost\":1.0}}},{\"bool\":{\"should\":[{\"bool\":{\"filter\":[{\"term\":{\"city\":{\"value\":\"Beverly Hills\",\"boost\":1.0}}},{\"term\":{\"state\":{\"value\":\"CA\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}},{\"term\":{\"zip\":{\"value\":\"90210\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}";
        String actual = XContentUtils.serializeAsJSON(resolversQuery);
        assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables.
     */
    @Test
    public void testPopulateMatcherClause() throws Exception {
        String matcherJson = "{\n" +
            "  \"clause\": {\n" +
            "    \"match\": {\n" +
            "      \"{{ field }}\": \"{{ value }}\"\n" +
            "    }" +
            "  }\n" +
            "}";
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        Map<String, String> params = new HashMap<>();
        QueryBuilder matcherClause = Job.buildMatcherClause(matcher, "field_phone", "555-123-4567", params);
        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"operator\":\"OR\",\"prefix_length\":0,\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\",\"auto_generate_synonyms_phrase_query\":true,\"boost\":1.0}}}";
        String actual = XContentUtils.serializeAsJSON(matcherClause);
        assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables.
     * Supply parameters that don't exist. Ensure they are ignored without failing the job.
     */
    @Test
    public void testPopulateMatcherClauseIgnoreUnusedParams() throws Exception {
        String matcherJson = "{\n" +
            "  \"clause\": {\n" +
            "    \"match\": {\n" +
            "      \"{{ field }}\": \"{{ value }}\"\n" +
            "    }" +
            "  }\n" +
            "}";
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        Map<String, String> params = new HashMap<>();
        params.put("foo", "bar");
        QueryBuilder matcherClause = Job.buildMatcherClause(matcher, "field_phone", "555-123-4567", params);
        String actual = XContentUtils.serializeAsJSON(matcherClause);
        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"operator\":\"OR\",\"prefix_length\":0,\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\",\"auto_generate_synonyms_phrase_query\":true,\"boost\":1.0}}}";
        assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables,
     * but don't include {{ field }} and expect an exception to be raised.
     */
    @Test(expected = ValidationException.class)
    public void testPopulateMatcherClauseFieldMissing() throws Exception {
        String matcherJson = "{\n" +
            "  \"clause\": {\n" +
            "    \"match\": {\n" +
            "      \"foo\": {\n" +
            "        \"query\": \"{{ value }}\",\n" +
            "        \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
            "      }\n" +
            "    }" +
            "  }\n" +
            "}";
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        Map<String, String> params = new HashMap<>();
        // Should throw
        Job.buildMatcherClause(matcher, "field_phone", "555-123-4567", params);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables,
     * but don't include {{ value }} and expect an exception to be raised.
     */
    @Test(expected = ValidationException.class)
    public void testPopulateMatcherClauseValueMissing() throws Exception {
        String matcherJson = "{\n" +
            "  \"clause\": {\n" +
            "    \"match\": {\n" +
            "      \"{{ field }}\": {\n" +
            "        \"query\": \"foo\",\n" +
            "        \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
            "      }\n" +
            "    }" +
            "  }\n" +
            "}";
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        Map<String, String> params = new HashMap<>();
        // should throw
        Job.buildMatcherClause(matcher, "field_phone", "555-123-4567", params);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables.
     * Use a matcher that defines a param but doesn't use it in the clause. Ignore it without failing the job.
     */
    @Test
    public void testPopulateMatcherClauseParamsUnrecognized() throws Exception {
        String matcherJson = "{\n" +
            "  \"clause\": {\n" +
            "    \"match\": {\n" +
            "      \"{{ field }}\": {\n" +
            "        \"query\": \"{{ value }}\",\n" +
            "        \"fuzziness\": \"2\"\n" +
            "      }\n" +
            "    }" +
            "  },\n" +
            "  \"params\": {" +
            "    \"foo\": \"bar\"" +
            "  }\n" +
            "}";
        Matcher matcher = new Matcher("matcher_phone", matcherJson);
        Map<String, String> params = new HashMap<>();
        QueryBuilder matcherClause = Job.buildMatcherClause(matcher, "field_phone", "555-123-4567", params);
        String actual = XContentUtils.serializeAsJSON(matcherClause);
        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"operator\":\"OR\",\"fuzziness\":\"2\",\"prefix_length\":0,\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\",\"auto_generate_synonyms_phrase_query\":true,\"boost\":1.0}}}";
        assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the input attribute.
     */
    @Test
    public void testPopulateMatcherClauseParamsFromInputAttribute() throws Exception {
        String attributes = "\"attributes\":{\"attribute_phone\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_phone\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_phone\": {\n" +
            "    \"clause\": {\n" +
            "      \"match\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"query\": \"{{ value }}\",\n" +
            "          \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
            "        }\n" +
            "      }" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_phone\": {\n" +
            "        \"attribute\": \"attribute_phone\", \"matcher\": \"matcher_phone\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_phone\": {\n" +
            "      \"values\": [ \"555-123-4567\" ],\n" +
            "      \"params\": { \"fuzziness\": 1 }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);

        List<QueryBuilder> attributeQueries = Job.buildAttributeQueries(
            input.model(), "index", input.attributes(), FILTER, false, new AtomicInteger()
        );
        assertEquals(1, attributeQueries.size());

        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"operator\":\"OR\",\"fuzziness\":\"1\",\"prefix_length\":0,\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\",\"auto_generate_synonyms_phrase_query\":true,\"boost\":1.0}}}";
        String actual = XContentUtils.serializeAsJSON(attributeQueries.get(0));
        assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the input attribute and overrides the params of a matcher.
     */
    @Test
    public void testPopulateMatcherClauseParamsFromInputAttributeOverridesMatcher() throws Exception {
        String attributes = "\"attributes\":{\"attribute_phone\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_phone\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_phone\": {\n" +
            "    \"clause\": {\n" +
            "      \"match\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"query\": \"{{ value }}\",\n" +
            "          \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
            "        }\n" +
            "      }" +
            "    },\n" +
            "    \"params\": {\n" +
            "      \"fuzziness\": 2\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_phone\": {\n" +
            "        \"attribute\": \"attribute_phone\", \"matcher\": \"matcher_phone\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_phone\": {\n" +
            "      \"values\": [ \"555-123-4567\" ],\n" +
            "      \"params\": { \"fuzziness\": 1 }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);

        List<QueryBuilder> attributeClauses = Job.buildAttributeQueries(
            input.model(), "index", input.attributes(), FILTER, false, new AtomicInteger()
        );
        assertEquals(1, attributeClauses.size());

        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"operator\":\"OR\",\"fuzziness\":\"1\",\"prefix_length\":0,\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\",\"auto_generate_synonyms_phrase_query\":true,\"boost\":1.0}}}";
        String actual = XContentUtils.serializeAsJSON(attributeClauses.get(0));
        assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the matcher.
     */
    @Test
    public void testPopulateMatcherClauseParamsFromMatcher() throws Exception {
        String attributes = "\"attributes\":{\"attribute_phone\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_phone\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_phone\": {\n" +
            "    \"clause\": {\n" +
            "      \"match\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"query\": \"{{ value }}\",\n" +
            "          \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
            "        }\n" +
            "      }" +
            "    },\n" +
            "    \"params\": {\n" +
            "      \"fuzziness\": 2\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_phone\": {\n" +
            "        \"attribute\": \"attribute_phone\", \"matcher\": \"matcher_phone\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_phone\": {\n" +
            "      \"values\": [ \"555-123-4567\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);

        List<QueryBuilder> attributeClauses = Job.buildAttributeQueries(
            input.model(), "index", input.attributes(), FILTER, false, new AtomicInteger()
        );
        assertEquals(1, attributeClauses.size());

        String expected = "{\"match\":{\"field_phone\":{\"query\":\"555-123-4567\",\"operator\":\"OR\",\"fuzziness\":\"2\",\"prefix_length\":0,\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\",\"auto_generate_synonyms_phrase_query\":true,\"boost\":1.0}}}";
        String actual = XContentUtils.serializeAsJSON(attributeClauses.get(0));
        assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the model attribute.
     */
    @Test
    public void testPopulateMatcherClauseParamsFromModelAttribute() throws Exception {
        String attributes = "\"attributes\": {" +
            "  \"attribute_timestamp\": {" +
            "    \"type\": \"date\",\n" +
            "    \"params\": {\n" +
            "      \"format\": \"yyyy-MM-dd'T'HH:mm:ss\",\n" +
            "      \"window\": \"30m\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_timestamp\": {\n" +
            "    \"clause\": {\n" +
            "      \"range\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
            "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
            "          \"format\": \"{{ params.format }}\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_timestamp\": {\n" +
            "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_timestamp\": {\n" +
            "      \"values\": [ \"123 Main St\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);

        List<QueryBuilder> attributeClauses = Job.buildAttributeQueries(
            input.model(), "index", input.attributes(), FILTER, false, new AtomicInteger()
        );
        assertEquals(1, attributeClauses.size());

        String expected = "{\"range\":{\"field_timestamp\":{\"from\":\"123 Main St||-30m\",\"to\":\"123 Main St||+30m\",\"include_lower\":true,\"include_upper\":true,\"format\":\"yyyy-MM-dd'T'HH:mm:ss\",\"boost\":1.0}}}";
        String actual = XContentUtils.serializeAsJSON(attributeClauses.get(0));
        assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the model attribute and overrides the params of a matcher.
     */
    @Test
    public void testPopulateMatcherClauseParamsFromModelAttributeOverridesMatcher() throws Exception {
        String attributes = "\"attributes\": {" +
            "  \"attribute_timestamp\": {" +
            "    \"type\": \"date\",\n" +
            "    \"params\": {\n" +
            "      \"format\": \"yyyy-MM-dd'T'HH:mm:ss\",\n" +
            "      \"window\": \"30m\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_timestamp\": {\n" +
            "    \"clause\": {\n" +
            "      \"range\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
            "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
            "          \"format\": \"{{ params.format }}\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"params\": {\n" +
            "      \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS\",\n" +
            "      \"window\": \"1h\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_timestamp\": {\n" +
            "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_timestamp\": {\n" +
            "      \"values\": [ \"123 Main St\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);

        List<QueryBuilder> attributeClauses = Job.buildAttributeQueries(
            input.model(), "index", input.attributes(), FILTER, false, new AtomicInteger()
        );
        assertEquals(1, attributeClauses.size());

        String expected = "{\"range\":{\"field_timestamp\":{\"from\":\"123 Main St||-30m\",\"to\":\"123 Main St||+30m\",\"include_lower\":true,\"include_upper\":true,\"format\":\"yyyy-MM-dd'T'HH:mm:ss\",\"boost\":1.0}}}";
        String actual = XContentUtils.serializeAsJSON(attributeClauses.get(0));
        assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * where the params are specified in the model attribute and input attribute and overrides the params of a matcher,
     * and where the input attribute takes precedence over the model attribute.
     */
    @Test
    public void testPopulateMatcherClauseParamsFromInputAttributeOverridesModelAttribute() throws Exception {
        String attributes = "\"attributes\": {" +
            "  \"attribute_timestamp\": {" +
            "    \"type\": \"date\",\n" +
            "    \"params\": {\n" +
            "      \"format\": \"yyyy-MM-dd'T'HH:mm:ss\",\n" +
            "      \"window\": \"30m\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_timestamp\": {\n" +
            "    \"clause\": {\n" +
            "      \"range\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
            "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
            "          \"format\": \"{{ params.format }}\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"params\": {\n" +
            "      \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS\",\n" +
            "      \"window\": \"1h\"\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_timestamp\": {\n" +
            "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_timestamp\": {\n" +
            "      \"values\": [ \"123 Main St\" ],\n" +
            "      \"params\": {\n" +
            "        \"format\": \"yyyy-MM-dd\",\n" +
            "        \"window\": \"15m\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);
        List<QueryBuilder> attributeClauses = Job.buildAttributeQueries(
            input.model(), "index", input.attributes(), FILTER, false, new AtomicInteger()
        );
        String expected = "{\"range\":{\"field_timestamp\":{\"from\":\"123 Main St||-15m\",\"to\":\"123 Main St||+15m\",\"include_lower\":true,\"include_upper\":true,\"format\":\"yyyy-MM-dd\",\"boost\":1.0}}}";
        String actual = XContentUtils.serializeAsJSON(attributeClauses.get(0));
        assertEquals(expected, actual);
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * but don't pass any values to the params and expect an exception to be raised.
     */
    @Test(expected = ValidationException.class)
    public void testPopulateMatcherClauseParamsMissing() throws Exception {
        String attributes = "\"attributes\":{\"attribute_phone\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_phone\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_phone\": {\n" +
            "    \"clause\": {\n" +
            "      \"match\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"query\": \"{{ value }}\",\n" +
            "          \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
            "        }\n" +
            "      }" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_phone\": {\n" +
            "        \"attribute\": \"attribute_phone\", \"matcher\": \"matcher_phone\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_phone\": {\n" +
            "      \"values\": [ \"555-123-4567\" ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);
        // should throw
        Job.buildAttributeQueries(
            input.model(), "index", input.attributes(), FILTER, false, new AtomicInteger()
        );
    }

    /**
     * Populate the clause of a matcher by substituting the {{ field }} and {{ value }} variables and any params,
     * but don't pass any values to the required params and expect an exception to be raised.
     */
    @Test(expected = ValidationException.class)
    public void testPopulateMatcherClauseParamsMismatched() throws Exception {
        String attributes = "\"attributes\":{\"attribute_phone\":{}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_phone\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_phone\": {\n" +
            "    \"clause\": {\n" +
            "      \"match\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"query\": \"{{ value }}\",\n" +
            "          \"fuzziness\": \"{{ params.fuzziness }}\"\n" +
            "        }\n" +
            "      }" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_phone\": {\n" +
            "        \"attribute\": \"attribute_phone\", \"matcher\": \"matcher_phone\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_phone\": {\n" +
            "      \"values\": [ \"555-123-4567\" ],\n" +
            "      \"params\": { \"foo\": \"bar\" }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);
        // should throw
        Job.buildAttributeQueries(
            input.model(), "index", input.attributes(), FILTER, false, new AtomicInteger()
        );
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified only in the
     * input attribute.
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatInputAttributeOnly() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\"}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_ip\": {\n" +
            "    \"clause\": {\n" +
            "      \"term\": {\n" +
            "        \"{{ field }}\": \"{{ value }}\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"matcher_timestamp\": {\n" +
            "    \"clause\": {\n" +
            "      \"range\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
            "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
            "          \"format\": \"{{ params.format }}\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_ip\": {\n" +
            "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
            "      },\n" +
            "      \"field_timestamp\": {\n" +
            "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_ip\": {\n" +
            "      \"values\": [\"192.168.0.1\"]\n" +
            "    },\n" +
            "    \"attribute_timestamp\": {\n" +
            "      \"values\": [ \"123 Main St\" ],\n" +
            "      \"params\": {\n" +
            "        \"format\": \"yyyy-MM-dd\",\n" +
            "        \"window\": \"15m\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);

        Map<String, Script> scriptMap = Job.buildScriptFields("index", input);
        assertTrue(scriptMap.containsKey("field_timestamp"));

        Script script = scriptMap.get("field_timestamp");
        assertEquals("painless", script.getLang());

        String expectedSource = "DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())";
        assertEquals(expectedSource, script.getIdOrCode());

        Map<String, Object> params = script.getParams();

        assertTrue(params.containsKey("format"));
        assertEquals("yyyy-MM-dd", params.get("format"));

        assertTrue(params.containsKey("field"));
        assertEquals("field_timestamp", params.get("field"));
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified only in the
     * matcher.
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatMatcherOnly() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\"}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_ip\": {\n" +
            "    \"clause\": {\n" +
            "      \"term\": {\n" +
            "        \"{{ field }}\": \"{{ value }}\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"matcher_timestamp\": {\n" +
            "    \"clause\": {\n" +
            "      \"range\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
            "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
            "          \"format\": \"{{ params.format }}\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"params\": {\n" +
            "      \"format\": \"yyyy-MM-dd\"" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_ip\": {\n" +
            "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
            "      },\n" +
            "      \"field_timestamp\": {\n" +
            "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_ip\": {\n" +
            "      \"values\": [\"192.168.0.1\"]\n" +
            "    },\n" +
            "    \"attribute_timestamp\": {\n" +
            "      \"values\": [ \"123 Main St\" ],\n" +
            "      \"params\": {\n" +
            "        \"format\": \"yyyy-MM-dd\",\n" +
            "        \"window\": \"15m\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);

        Map<String, Script> scriptMap = Job.buildScriptFields("index", input);
        assertTrue(scriptMap.containsKey("field_timestamp"));

        Script script = scriptMap.get("field_timestamp");
        assertEquals("painless", script.getLang());

        String expectedSource = "DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())";
        assertEquals(expectedSource, script.getIdOrCode());

        Map<String, Object> params = script.getParams();

        assertTrue(params.containsKey("format"));
        assertEquals("yyyy-MM-dd", params.get("format"));

        assertTrue(params.containsKey("field"));
        assertEquals("field_timestamp", params.get("field"));
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified only in the
     * model attribute.
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatModelAttributeOnly() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\",\"params\":{\"format\":\"yyyy-MM-dd\"}}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_ip\": {\n" +
            "    \"clause\": {\n" +
            "      \"term\": {\n" +
            "        \"{{ field }}\": \"{{ value }}\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"matcher_timestamp\": {\n" +
            "    \"clause\": {\n" +
            "      \"range\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
            "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
            "          \"format\": \"{{ params.format }}\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_ip\": {\n" +
            "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
            "      },\n" +
            "      \"field_timestamp\": {\n" +
            "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_ip\": {\n" +
            "      \"values\": [\"192.168.0.1\"]\n" +
            "    },\n" +
            "    \"attribute_timestamp\": {\n" +
            "      \"values\": [ \"123 Main St\" ],\n" +
            "      \"params\": {\n" +
            "        \"window\": \"15m\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);

        Map<String, Script> scriptMap = Job.buildScriptFields("index", input);
        assertTrue(scriptMap.containsKey("field_timestamp"));

        Script script = scriptMap.get("field_timestamp");
        assertEquals("painless", script.getLang());

        String expectedSource = "DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())";
        assertEquals(expectedSource, script.getIdOrCode());

        Map<String, Object> params = script.getParams();

        assertTrue(params.containsKey("format"));
        assertEquals("yyyy-MM-dd", params.get("format"));

        assertTrue(params.containsKey("field"));
        assertEquals("field_timestamp", params.get("field"));
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified both in the
     * model attribute and the matcher. The param of the model attribute should override the param of the matcher.
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatModelAttributeOverridesMatcher() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\",\"params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_ip\": {\n" +
            "    \"clause\": {\n" +
            "      \"term\": {\n" +
            "        \"{{ field }}\": \"{{ value }}\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"matcher_timestamp\": {\n" +
            "    \"clause\": {\n" +
            "      \"range\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
            "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
            "          \"format\": \"{{ params.format }}\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"params\": {\n" +
            "      \"format\": \"yyyy-MM-dd\"" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_ip\": {\n" +
            "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
            "      },\n" +
            "      \"field_timestamp\": {\n" +
            "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_ip\": {\n" +
            "      \"values\": [\"192.168.0.1\"]\n" +
            "    },\n" +
            "    \"attribute_timestamp\": {\n" +
            "      \"values\": [ \"123 Main St\" ],\n" +
            "      \"params\": {\n" +
            "        \"window\": \"15m\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);

        Map<String, Script> scriptMap = Job.buildScriptFields("index", input);
        assertTrue(scriptMap.containsKey("field_timestamp"));

        Script script = scriptMap.get("field_timestamp");
        assertEquals("painless", script.getLang());

        String expectedSource = "DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())";
        assertEquals(expectedSource, script.getIdOrCode());

        Map<String, Object> params = script.getParams();

        assertTrue(params.containsKey("format"));
        assertEquals("yyyy-MM-dd'T'HH:mm:ss", params.get("format"));

        assertTrue(params.containsKey("field"));
        assertEquals("field_timestamp", params.get("field"));
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified both in the
     * input attribute and the model attribute. The param of the input attribute should override the param of the
     * model attribute.
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatInputAttributeOverridesModelAttribute() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\",\"params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_ip\": {\n" +
            "    \"clause\": {\n" +
            "      \"term\": {\n" +
            "        \"{{ field }}\": \"{{ value }}\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"matcher_timestamp\": {\n" +
            "    \"clause\": {\n" +
            "      \"range\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
            "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
            "          \"format\": \"{{ params.format }}\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"params\": {\n" +
            "      \"format\": \"yyyy-MM-dd\"" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_ip\": {\n" +
            "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
            "      },\n" +
            "      \"field_timestamp\": {\n" +
            "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_ip\": {\n" +
            "      \"values\": [\"192.168.0.1\"]\n" +
            "    },\n" +
            "    \"attribute_timestamp\": {\n" +
            "      \"values\": [ \"123 Main St\" ],\n" +
            "      \"params\": {\n" +
            "        \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS\",\n" +
            "        \"window\": \"15m\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);

        Map<String, Script> scriptMap = Job.buildScriptFields("index", input);
        assertTrue(scriptMap.containsKey("field_timestamp"));

        Script script = scriptMap.get("field_timestamp");
        assertEquals("painless", script.getLang());

        String expectedSource = "DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())";
        assertEquals(expectedSource, script.getIdOrCode());

        Map<String, Object> params = script.getParams();

        assertTrue(params.containsKey("format"));
        assertEquals("yyyy-MM-dd'T'HH:mm:ss.SSS", params.get("format"));

        assertTrue(params.containsKey("field"));
        assertEquals("field_timestamp", params.get("field"));
    }

    /**
     * Make the "script_fields" clause for a "date" type attribute where the "format" param is specified both in the
     * input attribute and the model attribute, but the value of the input attribute param is null. The param of the
     * input attribute should not override the non-null param of the model attribute.
     */
    @Test
    public void testMakeScriptFieldsClauseTypeDateFormatNullNotOverrides() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\",\"params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss\"}}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_ip\": {\n" +
            "    \"clause\": {\n" +
            "      \"term\": {\n" +
            "        \"{{ field }}\": \"{{ value }}\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"matcher_timestamp\": {\n" +
            "    \"clause\": {\n" +
            "      \"range\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
            "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
            "          \"format\": \"{{ params.format }}\"\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"params\": {\n" +
            "      \"format\": \"yyyy-MM-dd\"" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_ip\": {\n" +
            "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
            "      },\n" +
            "      \"field_timestamp\": {\n" +
            "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_ip\": {\n" +
            "      \"values\": [\"192.168.0.1\"]\n" +
            "    },\n" +
            "    \"attribute_timestamp\": {\n" +
            "      \"values\": [ \"123 Main St\" ],\n" +
            "      \"params\": {\n" +
            "        \"format\": null,\n" +
            "        \"window\": \"15m\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);

        Map<String, Script> scriptMap = Job.buildScriptFields("index", input);
        assertTrue(scriptMap.containsKey("field_timestamp"));

        Script script = scriptMap.get("field_timestamp");
        assertEquals("painless", script.getLang());

        String expectedSource = "DateFormat df = new SimpleDateFormat(params.format); df.setTimeZone(TimeZone.getTimeZone('UTC')); return df.format(doc[params.field].value.toInstant().toEpochMilli())";
        assertEquals(expectedSource, script.getIdOrCode());

        Map<String, Object> params = script.getParams();

        assertTrue(params.containsKey("format"));
        assertEquals("yyyy-MM-dd'T'HH:mm:ss", params.get("format"));

        assertTrue(params.containsKey("field"));
        assertEquals("field_timestamp", params.get("field"));
    }

    /**
     * The "script_fields" clause for a "date" type attribute must throw an exception if the "format" param is missing
     * from the matcher, the model attribute, and the input attribute.
     */
    @Test(expected = ValidationException.class)
    public void testMakeScriptFieldsClauseTypeDateFormatMissing() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\"}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_ip\": {\n" +
            "    \"clause\": {\n" +
            "      \"term\": {\n" +
            "        \"{{ field }}\": \"{{ value }}\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"matcher_timestamp\": {\n" +
            "    \"clause\": {\n" +
            "      \"range\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
            "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
            "          \"format\": \"{{ params.format }}\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_ip\": {\n" +
            "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
            "      },\n" +
            "      \"field_timestamp\": {\n" +
            "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_ip\": {\n" +
            "      \"values\": [\"192.168.0.1\"]\n" +
            "    },\n" +
            "    \"attribute_timestamp\": {\n" +
            "      \"values\": [ \"123 Main St\" ],\n" +
            "      \"params\": {\n" +
            "        \"window\": \"15m\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);
        // should throw
        Job.buildScriptFields("index", input).get("field_timestamp");
    }

    /**
     * The "script_fields" clause for a "date" type attribute must throw an exception if the only "format" param is null.
     */
    @Test(expected = ValidationException.class)
    public void testMakeScriptFieldsClauseTypeDateFormatNull() throws Exception {
        String attributes = "\"attributes\":{\"attribute_ip\":{},\"attribute_timestamp\":{\"type\":\"date\"}}";
        String resolvers = "\"resolvers\":{\"a\":{\"attributes\":[\"attribute_ip\",\"attribute_timestamp\"]}}";
        String matchers = "\"matchers\":{\n" +
            "  \"matcher_ip\": {\n" +
            "    \"clause\": {\n" +
            "      \"term\": {\n" +
            "        \"{{ field }}\": \"{{ value }}\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"matcher_timestamp\": {\n" +
            "    \"clause\": {\n" +
            "      \"range\": {\n" +
            "        \"{{ field }}\": {\n" +
            "          \"gte\": \"{{ value }}||-{{ params.window }}\",\n" +
            "          \"lte\": \"{{ value }}||+{{ params.window }}\",\n" +
            "          \"format\": \"{{ params.format }}\"\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        String indices = "\"indices\": {\n" +
            "  \"index\": {\n" +
            "    \"fields\": {\n" +
            "      \"field_ip\": {\n" +
            "        \"attribute\": \"attribute_ip\", \"matcher\":\"matcher_ip\"\n" +
            "      },\n" +
            "      \"field_timestamp\": {\n" +
            "        \"attribute\": \"attribute_timestamp\", \"matcher\": \"matcher_timestamp\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Model model = new Model("{" + attributes + "," + resolvers + "," + matchers + "," + indices + "}");
        String json = "{\n" +
            "  \"attributes\": {\n" +
            "    \"attribute_ip\": {\n" +
            "      \"values\": [\"192.168.0.1\"]\n" +
            "    },\n" +
            "    \"attribute_timestamp\": {\n" +
            "      \"values\": [ \"123 Main St\" ],\n" +
            "      \"params\": {\n" +
            "        \"format\": null,\n" +
            "        \"window\": \"15m\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
        Input input = new Input(json, model);
        // should throw
        Job.buildScriptFields("index", input).get("field_timestamp");
    }

    /**
     * Test various calculations of the attribute identity confidence score.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testCalculateAttributeIdentityConfidenceScore() {

        // When all quality scores are 1.0,the output must be equal to the base score
        assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 1.00, 1.00), 0.75, 0.0000000001);

        // When any quality score is 0.0, the output must be 0.5
        assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 1.00, 0.00), 0.50, 0.0000000001);
        assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.90, 0.00), 0.50, 0.0000000001);
        assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.00, 0.00), 0.50, 0.0000000001);

        // The order of the quality scores must not matter
        assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.90, 0.80), 0.68, 0.0000000001);
        assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.80, 0.90), 0.68, 0.0000000001);

        // Any null quality scores must be omitted
        assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.90, null), 0.725, 0.0000000001);
        assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, null, 0.8), 0.70, 0.0000000001);
        assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, null, null), 0.75, 0.0000000001);

        // When the base score is null, the output must be null
        assertNull(Job.calculateAttributeIdentityConfidenceScore(null, 0.9, 0.8));
        assertNull(Job.calculateAttributeIdentityConfidenceScore(null, 0.9, null));
        assertNull(Job.calculateAttributeIdentityConfidenceScore(null, null, 0.8));
        assertNull(Job.calculateAttributeIdentityConfidenceScore(null, null, null));

        // Various tests
        assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.75, 0.625, 0.99), 0.6546875, 0.0000000001);
        assertEquals(Job.calculateAttributeIdentityConfidenceScore(0.87, 0.817, 0.93), 0.7811297, 0.0000000001);
    }

    /**
     * Test various calculations of the composite identity confidence score.
     */
    @Test
    public void testCalculateCompositeIdentityConfidenceScore() {

        // Inputs of 1.0 must always produce an output of 1.0
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 1.00)), 1.00000000000, 0.0000000001);

        // Inputs of 0.5 or null must not affect the output score
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.55, 0.65, 0.75)), 0.87195121951, 0.0000000001);
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.55, 0.65, 0.75, 0.50)), 0.87195121951, 0.0000000001);
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.55, 0.65, 0.75, null)), 0.87195121951, 0.0000000001);

        // Inputs of 0.0 must always produce an output of 0.0
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 0.00)), 0.00000000000, 0.0000000001);

        // Inputs of 1.0 and 0.0 together must always produce an output of 0.5
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 1.00, 0.00)), 0.50000000000, 0.0000000001);

        // Output score must be null given an empty list of input scores.
        List<Double> scores = new ArrayList<>();
        assertNull(Job.calculateCompositeIdentityConfidenceScore(scores));

        // Output score must be null given only null input scores.
        Double nullScore = null;
        assertNull(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(nullScore, nullScore)));

        // The order of the inputs must not matter
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.55, 0.75, 0.65)), 0.87195121951, 0.0000000001);
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.65, 0.55, 0.75)), 0.87195121951, 0.0000000001);
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.65, 0.75, 0.55)), 0.87195121951, 0.0000000001);
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 0.65, 0.55)), 0.87195121951, 0.0000000001);
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 0.55, 0.65)), 0.87195121951, 0.0000000001);

        // Various tests
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 0.95)), 0.98275862069, 0.0000000001);
        assertEquals(Job.calculateCompositeIdentityConfidenceScore(Arrays.asList(0.75, 0.85)), 0.94444444444, 0.0000000001);
    }

}
