package org.elasticsearch.plugin.zentity;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import static io.zentity.devtools.JsonTestUtil.assertUnorderedEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResolutionActionIT extends AbstractActionITCase {
    private final StringEntity TEST_PAYLOAD_JOB_NO_SCOPE = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_a\": [ \"a_00\" ]\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_ATTRIBUTES = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_a\": [ \"a_00\" ]\n" +
        "  },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\" ],\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"a_00\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\" ],\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_EXPLANATION = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_a\": [ \"a_00\" ],\n" +
        "    \"attribute_type_date\": {" +
        "      \"values\": [ \"1999-12-31T23:59:57.0000\" ],\n" +
        "      \"params\": {\n" +
        "        \"format\" : \"yyyy-MM-dd'T'HH:mm:ss.0000\",\n" +
        "        \"window\" : \"1d\"\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_EXPLANATION_TERMS = new StringEntity("{\n" +
        "  \"attributes\": {" +
        "    \"attribute_type_date\": {" +
        "      \"params\": {\n" +
        "        \"format\" : \"yyyy-MM-dd'T'HH:mm:ss.0000\",\n" +
        "        \"window\" : \"1d\"\n" +
        "      }\n" +
        "    }\n" +
        "  },\n" +
        "  \"terms\": [ \"a_00\", \"1999-12-31T23:59:57.0000\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_IDS = new StringEntity("{\n" +
        "  \"ids\": {\n" +
        "    \"zentity_test_index_a\": [ \"a0\" ]\n" +
        "  },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\" ],\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_ATTRIBUTES_IDS = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_a\": [ \"a_00\" ]\n" +
        "  },\n" +
        "  \"ids\": {\n" +
        "    \"zentity_test_index_a\": [ \"a6\" ]\n" +
        "  },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\" ],\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_TERMS_IDS = new StringEntity("{\n" +
        "  \"ids\": {\n" +
        "    \"zentity_test_index_a\": [ \"a6\" ]\n" +
        "  },\n" +
        "  \"terms\": [ \"a_00\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\" ],\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_MAX_HOPS_AND_DOCS = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_d\": { \"values\": [ \"d_00\" ] }\n" +
        "  },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_TRUE = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_boolean\": [ true ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_boolean\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_TRUE_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"true\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_boolean\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_DATE = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_d\": { \"values\": [ \"d_00\" ] },\n" +
        "    \"attribute_type_date\": {\n" +
        "      \"values\": [ \"2000-01-01 00:00:00\" ],\n" +
        "      \"params\": { \"format\": \"yyyy-MM-dd HH:mm:ss\", \"window\": \"1s\" }\n" +
        "    }\n" +
        "  },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"resolvers\": [ \"resolver_type_date_a\", \"resolver_type_date_b\", \"resolver_type_date_c\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_DATE_TERMS = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_type_date\": {\n" +
        "      \"params\": { \"format\": \"yyyy-MM-dd HH:mm:ss\", \"window\": \"1s\" }\n" +
        "    }\n" +
        "  },\n" +
        "  \"terms\": [ \"d_00\", \"2000-01-01 00:00:00\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"resolvers\": [ \"resolver_type_date_a\", \"resolver_type_date_b\", \"resolver_type_date_c\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_FALSE = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_boolean\": [ false ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_boolean\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_FALSE_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"false\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_boolean\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_POSITIVE = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_double\": [ 3.141592653589793 ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_double\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_POSITIVE_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"3.141592653589793\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_double\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_NEGATIVE = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_double\": [ -3.141592653589793 ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_double\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_NEGATIVE_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"-3.141592653589793\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_double\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_POSITIVE = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_float\": [ 1.0 ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_float\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_POSITIVE_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"1.0\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_float\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_NEGATIVE = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_float\": [ -1.0 ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_float\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_NEGATIVE_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"-1.0\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_float\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_POSITIVE = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_integer\": [ 1 ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_integer\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_POSITIVE_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"1\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_integer\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_NEGATIVE = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_integer\": [ -1 ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_integer\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_NEGATIVE_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"-1\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_integer\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_POSITIVE = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_long\": [ 922337203685477 ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_long\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_POSITIVE_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"922337203685477\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_long\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_NEGATIVE = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_long\": [ -922337203685477 ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_long\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_NEGATIVE_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"-922337203685477\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_long\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_STRING_A = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_string\": [ \"a\" ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_string\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_STRING_A_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"a\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_string\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_STRING_B = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_type_string\": [ \"b\" ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_string\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_DATA_TYPES_STRING_B_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"b\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_type_string\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_OBJECT = new StringEntity("{\n" +
        "  \"attributes\": { \"attribute_object\": [ \"a\" ] },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ], \"resolvers\": [ \"resolver_object\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_ATTRIBUTES = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_a\": [ \"a_00\" ]\n" +
        "  },\n" +
        "  \"scope\": {\n" +
        "    \"exclude\": {\n" +
        "      \"attributes\": { \"attribute_a\":[ \"a_11\" ], \"attribute_c\": [ \"c_03\" ] }\n" +
        "    },\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\" ],\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_ATTRIBUTES_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"a_00\" ],\n" +
        "  \"scope\": {\n" +
        "    \"exclude\": {\n" +
        "      \"attributes\": { \"attribute_a\":[ \"a_11\" ], \"attribute_c\": [ \"c_03\" ] }\n" +
        "    },\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\" ],\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_SCOPE_INCLUDE_ATTRIBUTES = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_d\": [ \"d_00\" ]\n" +
        "  },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"attributes\": { \"attribute_d\": [ \"d_00\" ], \"attribute_type_double\": [ 3.141592653589793 ] },\n" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\", \"zentity_test_index_d\" ],\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_SCOPE_INCLUDE_ATTRIBUTES_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"d_00\" ],\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"attributes\": { \"attribute_d\": [ \"d_00\" ], \"attribute_type_double\": [ 3.141592653589793 ] },\n" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\", \"zentity_test_index_d\" ],\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_AND_INCLUDE_ATTRIBUTES = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_d\": [ \"d_00\" ]\n" +
        "  },\n" +
        "  \"scope\": {\n" +
        "    \"exclude\": {\n" +
        "      \"attributes\": { \"attribute_c\": [ \"c_00\", \"c_01\" ] }\n" +
        "    },\n" +
        "    \"include\": {\n" +
        "      \"attributes\": { \"attribute_d\": [ \"d_00\" ], \"attribute_type_double\": [ 3.141592653589793 ] },\n" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\", \"zentity_test_index_d\" ],\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_AND_INCLUDE_ATTRIBUTES_TERMS = new StringEntity("{\n" +
        "  \"terms\": [ \"d_00\" ],\n" +
        "  \"scope\": {\n" +
        "    \"exclude\": {\n" +
        "      \"attributes\": { \"attribute_c\": [ \"c_00\", \"c_01\" ] }\n" +
        "    },\n" +
        "    \"include\": {\n" +
        "      \"attributes\": { \"attribute_d\": [ \"d_00\" ], \"attribute_type_double\": [ 3.141592653589793 ] },\n" +
        "      \"indices\": [ \"zentity_test_index_a\", \"zentity_test_index_b\", \"zentity_test_index_c\", \"zentity_test_index_d\" ],\n" +
        "      \"resolvers\": [ \"resolver_a\", \"resolver_b\", \"resolver_c\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_RESOLVER_WEIGHT = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_a\": [ \"a_10\" ],\n" +
        "    \"attribute_b\": [ \"b_10\" ]\n" +
        "  },\n" +
        "  \"scope\": {\n" +
        "    \"include\": {\n" +
        "      \"indices\": [ \"zentity_test_index_a\" ]\n" +
        "    }\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_ERROR = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"attribute_a\": [ \"a_10\" ],\n" +
        "    \"attribute_b\": [ \"b_10\" ]\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    private final StringEntity TEST_PAYLOAD_JOB_ARRAYS = new StringEntity("{\n" +
        "  \"attributes\": {\n" +
        "    \"string\": [ \"abc\" ],\n" +
        "    \"array\": [ \"222\" ]\n" +
        "  }\n" +
        "}", ContentType.APPLICATION_JSON);

    @Test
    public void testJobNoScope() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_NO_SCOPE);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(40, json.get("hits").get("total").asInt());
            JsonPointer pathAttributes = JsonPointer.compile("/_attributes");
            JsonPointer pathNull = JsonPointer.compile("/_attributes/attribute_type_string_null");
            JsonPointer pathUnused = JsonPointer.compile("/_attributes/attribute_type_string_unused");
            for (JsonNode doc : json.get("hits").get("hits")) {
                assertFalse(doc.at(pathAttributes).isMissingNode());
                assertTrue(doc.at(pathNull).isMissingNode());
                assertTrue(doc.at(pathUnused).isMissingNode());
            }
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobAttributes() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_ATTRIBUTES);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(6, json.get("hits").get("total").asInt());
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("b0,0");
            docsExpected.add("c0,1");
            docsExpected.add("a1,2");
            docsExpected.add("b1,3");
            docsExpected.add("c1,4");
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobTerms() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_TERMS);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(6, json.get("hits").get("total").asInt());
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("b0,0");
            docsExpected.add("c0,1");
            docsExpected.add("a1,2");
            docsExpected.add("b1,3");
            docsExpected.add("c1,4");
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobExplanation() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.addParameter("_attributes", "false");
            postResolution.addParameter("_explanation", "true");
            postResolution.addParameter("_source", "false");
            postResolution.addParameter("max_hops", "1");
            postResolution.addParameter("max_docs_per_query", "2");
            postResolution.setEntity(TEST_PAYLOAD_JOB_EXPLANATION);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(3, json.get("hits").get("total").asInt());
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("a1,1");
            docsExpected.add("a2,1");
            assertEquals(docsExpected, getActualIdHits(json));
            for (JsonNode hit : json.get("hits").get("hits")) {
                String expectedExplanationJson = "";
                switch (hit.get("_id").asText()) {
                    case "a0":
                        expectedExplanationJson = "{\"resolvers\":{\"resolver_a\":{\"attributes\":[\"attribute_a\"]},\"resolver_type_date_a\":{\"attributes\":[\"attribute_type_date\",\"attribute_a\"]}},\"matches\":[{\"attribute\":\"attribute_a\",\"target_field\":\"field_a.keyword\",\"target_value\":\"a_00\",\"input_value\":\"a_00\",\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_a\",\"target_field\":\"field_a.clean\",\"target_value\":\"a_00\",\"input_value\":\"a_00\",\"input_matcher\":\"matcher_a\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_date\",\"target_field\":\"type_date\",\"target_value\":\"1999-12-31T23:59:57.0000\",\"input_value\":\"1999-12-31T23:59:57.0000\",\"input_matcher\":\"matcher_c\",\"input_matcher_params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss.0000\",\"window\":\"1d\"}}]}";
                        break;
                    case "a1":
                        expectedExplanationJson = "{\"resolvers\":{\"resolver_c\":{\"attributes\":[\"attribute_d\"]},\"resolver_type_date_c\":{\"attributes\":[\"attribute_d\",\"attribute_type_date\"]}},\"matches\":[{\"attribute\":\"attribute_d\",\"target_field\":\"field_d.keyword\",\"target_value\":\"d_00\",\"input_value\":\"d_00\",\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_date\",\"target_field\":\"type_date\",\"target_value\":\"1999-12-31T23:59:59.0000\",\"input_value\":\"1999-12-31T23:59:57.0000\",\"input_matcher\":\"matcher_c\",\"input_matcher_params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss.0000\",\"window\":\"1d\"}},{\"attribute\":\"attribute_d\",\"target_field\":\"field_d.clean\",\"target_value\":\"d_00\",\"input_value\":\"d_00\",\"input_matcher\":\"matcher_a\",\"input_matcher_params\":{}}]}";
                        break;
                    case "a2":
                        expectedExplanationJson = "{\"resolvers\":{\"resolver_c\":{\"attributes\":[\"attribute_d\"]},\"resolver_type_boolean\":{\"attributes\":[\"attribute_type_boolean\"]},\"resolver_type_float\":{\"attributes\":[\"attribute_type_float\"]},\"resolver_type_integer\":{\"attributes\":[\"attribute_type_integer\"]},\"resolver_type_string\":{\"attributes\":[\"attribute_type_string\"]},\"resolver_type_double\":{\"attributes\":[\"attribute_type_double\"]},\"resolver_type_date_c\":{\"attributes\":[\"attribute_d\",\"attribute_type_date\"]},\"resolver_type_long\":{\"attributes\":[\"attribute_type_long\"]},\"resolver_object\":{\"attributes\":[\"attribute_object\"]}},\"matches\":[{\"attribute\":\"attribute_type_double\",\"target_field\":\"type_double\",\"target_value\":3.141592653589793,\"input_value\":3.141592653589793,\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_d\",\"target_field\":\"field_d.keyword\",\"target_value\":\"d_00\",\"input_value\":\"d_00\",\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_object\",\"target_field\":\"object.a.b.c.keyword\",\"target_value\":\"a\",\"input_value\":\"a\",\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_date\",\"target_field\":\"type_date\",\"target_value\":\"2000-01-01T00:00:00.0000\",\"input_value\":\"1999-12-31T23:59:57.0000\",\"input_matcher\":\"matcher_c\",\"input_matcher_params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss.0000\",\"window\":\"1d\"}},{\"attribute\":\"attribute_type_boolean\",\"target_field\":\"type_boolean\",\"target_value\":true,\"input_value\":true,\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_float\",\"target_field\":\"type_float\",\"target_value\":1.0,\"input_value\":1.0,\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_integer\",\"target_field\":\"type_integer\",\"target_value\":1,\"input_value\":1,\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_d\",\"target_field\":\"field_d.clean\",\"target_value\":\"d_00\",\"input_value\":\"d_00\",\"input_matcher\":\"matcher_a\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_long\",\"target_field\":\"type_long\",\"target_value\":922337203685477,\"input_value\":922337203685477,\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_string\",\"target_field\":\"type_string\",\"target_value\":\"a\",\"input_value\":\"a\",\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}}]}";
                        break;
                }
                JsonNode expected = Json.ORDERED_MAPPER.readTree(expectedExplanationJson);
                JsonNode actual = hit.get("_explanation");
                assertUnorderedEquals("explanation", expected, actual);
            }
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobExplanationTerms() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.addParameter("_attributes", "false");
            postResolution.addParameter("_explanation", "true");
            postResolution.addParameter("_source", "false");
            postResolution.addParameter("max_hops", "1");
            postResolution.addParameter("max_docs_per_query", "2");
            postResolution.setEntity(TEST_PAYLOAD_JOB_EXPLANATION_TERMS);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(3, json.get("hits").get("total").asInt());
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("a1,1");
            docsExpected.add("a2,1");
            assertEquals(docsExpected, getActualIdHits(json));

            for (JsonNode doc : json.get("hits").get("hits")) {
                String expectedJson = "";
                switch (doc.get("_id").asText()) {
                    case "a0":
                        expectedJson = "{\"resolvers\":{\"resolver_a\":{\"attributes\":[\"attribute_a\"]},\"resolver_type_date_a\":{\"attributes\":[\"attribute_type_date\",\"attribute_a\"]}},\"matches\":[{\"attribute\":\"attribute_a\",\"target_field\":\"field_a.clean\",\"target_value\":\"a_00\",\"input_value\":\"a_00\",\"input_matcher\":\"matcher_a\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_a\",\"target_field\":\"field_a.keyword\",\"target_value\":\"a_00\",\"input_value\":\"a_00\",\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_date\",\"target_field\":\"type_date\",\"target_value\":\"1999-12-31T23:59:57.0000\",\"input_value\":\"1999-12-31T23:59:57.0000\",\"input_matcher\":\"matcher_c\",\"input_matcher_params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss.0000\",\"window\":\"1d\"}}]}";
                        break;
                    case "a1":
                        expectedJson = "{\"resolvers\":{\"resolver_c\":{\"attributes\":[\"attribute_d\"]},\"resolver_type_date_c\":{\"attributes\":[\"attribute_d\",\"attribute_type_date\"]}},\"matches\":[{\"attribute\":\"attribute_d\",\"target_field\":\"field_d.keyword\",\"target_value\":\"d_00\",\"input_value\":\"d_00\",\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_date\",\"target_field\":\"type_date\",\"target_value\":\"1999-12-31T23:59:59.0000\",\"input_value\":\"1999-12-31T23:59:57.0000\",\"input_matcher\":\"matcher_c\",\"input_matcher_params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss.0000\",\"window\":\"1d\"}},{\"attribute\":\"attribute_d\",\"target_field\":\"field_d.clean\",\"target_value\":\"d_00\",\"input_value\":\"d_00\",\"input_matcher\":\"matcher_a\",\"input_matcher_params\":{}}]}";
                        break;
                    case "a2":
                        expectedJson = "{\"resolvers\":{\"resolver_c\":{\"attributes\":[\"attribute_d\"]},\"resolver_type_boolean\":{\"attributes\":[\"attribute_type_boolean\"]},\"resolver_type_float\":{\"attributes\":[\"attribute_type_float\"]},\"resolver_type_integer\":{\"attributes\":[\"attribute_type_integer\"]},\"resolver_type_string\":{\"attributes\":[\"attribute_type_string\"]},\"resolver_type_double\":{\"attributes\":[\"attribute_type_double\"]},\"resolver_type_date_c\":{\"attributes\":[\"attribute_d\",\"attribute_type_date\"]},\"resolver_type_long\":{\"attributes\":[\"attribute_type_long\"]},\"resolver_object\":{\"attributes\":[\"attribute_object\"]}},\"matches\":[{\"attribute\":\"attribute_type_double\",\"target_field\":\"type_double\",\"target_value\":3.141592653589793,\"input_value\":3.141592653589793,\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_d\",\"target_field\":\"field_d.keyword\",\"target_value\":\"d_00\",\"input_value\":\"d_00\",\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_object\",\"target_field\":\"object.a.b.c.keyword\",\"target_value\":\"a\",\"input_value\":\"a\",\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_date\",\"target_field\":\"type_date\",\"target_value\":\"2000-01-01T00:00:00.0000\",\"input_value\":\"1999-12-31T23:59:57.0000\",\"input_matcher\":\"matcher_c\",\"input_matcher_params\":{\"format\":\"yyyy-MM-dd'T'HH:mm:ss.0000\",\"window\":\"1d\"}},{\"attribute\":\"attribute_type_boolean\",\"target_field\":\"type_boolean\",\"target_value\":true,\"input_value\":true,\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_float\",\"target_field\":\"type_float\",\"target_value\":1.0,\"input_value\":1.0,\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_integer\",\"target_field\":\"type_integer\",\"target_value\":1,\"input_value\":1,\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_d\",\"target_field\":\"field_d.clean\",\"target_value\":\"d_00\",\"input_value\":\"d_00\",\"input_matcher\":\"matcher_a\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_long\",\"target_field\":\"type_long\",\"target_value\":922337203685477,\"input_value\":922337203685477,\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}},{\"attribute\":\"attribute_type_string\",\"target_field\":\"type_string\",\"target_value\":\"a\",\"input_value\":\"a\",\"input_matcher\":\"matcher_b\",\"input_matcher_params\":{}}]}";
                        break;
                }
                JsonNode expected = Json.ORDERED_MAPPER.readTree(expectedJson);
                JsonNode actual = doc.get("_explanation");
                assertUnorderedEquals("explanation", expected, actual);
            }
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobIds() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request req = new Request("POST", endpoint);
            req.setEntity(TEST_PAYLOAD_JOB_IDS);
            Response response = client.performRequest(req);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(6, json.get("hits").get("total").asInt());
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("b0,1");
            docsExpected.add("c0,2");
            docsExpected.add("a1,3");
            docsExpected.add("b1,4");
            docsExpected.add("c1,5");
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobAttributesIds() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request req = new Request("POST", endpoint);
            req.setEntity(TEST_PAYLOAD_JOB_ATTRIBUTES_IDS);
            Response response = client.performRequest(req);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(30, json.get("hits").get("total").asInt());
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0"); // check
            docsExpected.add("a6,0");
            docsExpected.add("b0,0"); // check
            docsExpected.add("a2,1");
            docsExpected.add("a7,1");
            docsExpected.add("a8,1");
            docsExpected.add("a9,1");
            docsExpected.add("b2,1");
            docsExpected.add("b6,1");
            docsExpected.add("b7,1");
            docsExpected.add("b8,1");
            docsExpected.add("b9,1");
            docsExpected.add("c0,1");
            docsExpected.add("c2,1");
            docsExpected.add("c6,1");
            docsExpected.add("c7,1");
            docsExpected.add("c8,1");
            docsExpected.add("c9,1");
            docsExpected.add("a1,2"); // check
            docsExpected.add("a3,2");
            docsExpected.add("a4,2");
            docsExpected.add("a5,2");
            docsExpected.add("b3,2");
            docsExpected.add("b4,2");
            docsExpected.add("b5,2");
            docsExpected.add("c3,2");
            docsExpected.add("c4,2");
            docsExpected.add("c5,2");
            docsExpected.add("b1,3"); // check
            docsExpected.add("c1,4"); // check
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobTermsIds() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request req = new Request("POST", endpoint);
            req.addParameter("queries", "true");
            req.setEntity(TEST_PAYLOAD_JOB_TERMS_IDS);
            Response response = client.performRequest(req);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(30, json.get("hits").get("total").asInt());
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("a6,0");
            docsExpected.add("b0,0");
            docsExpected.add("a2,1");
            docsExpected.add("a7,1");
            docsExpected.add("a8,1");
            docsExpected.add("a9,1");
            docsExpected.add("b2,1");
            docsExpected.add("b6,1");
            docsExpected.add("b7,1");
            docsExpected.add("b8,1");
            docsExpected.add("b9,1");
            docsExpected.add("c0,1");
            docsExpected.add("c2,1");
            docsExpected.add("c6,1");
            docsExpected.add("c7,1");
            docsExpected.add("c8,1");
            docsExpected.add("c9,1");
            docsExpected.add("a1,2");
            docsExpected.add("a3,2");
            docsExpected.add("a4,2");
            docsExpected.add("a5,2");
            docsExpected.add("b3,2");
            docsExpected.add("b4,2");
            docsExpected.add("b5,2");
            docsExpected.add("c3,2");
            docsExpected.add("c4,2");
            docsExpected.add("c5,2");
            docsExpected.add("b1,3");
            docsExpected.add("c1,4");
            Set<String> actualHits = getActualIdHits(json);
            assertEquals(docsExpected, actualHits);
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobMaxHopsAndDocs() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.addParameter("max_hops", "2");
            postResolution.addParameter("max_docs_per_query", "2");
            postResolution.setEntity(TEST_PAYLOAD_JOB_MAX_HOPS_AND_DOCS);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(20, json.get("hits").get("total").asInt());
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("a1,0");
            docsExpected.add("b0,0");
            docsExpected.add("b1,0");
            docsExpected.add("c0,0");
            docsExpected.add("c1,0");
            docsExpected.add("d0,0");
            docsExpected.add("d1,0");
            docsExpected.add("a2,1");
            docsExpected.add("b2,1");
            docsExpected.add("c2,1");
            docsExpected.add("d2,1");
            docsExpected.add("a3,2");
            docsExpected.add("a4,2");
            docsExpected.add("b3,2");
            docsExpected.add("b4,2");
            docsExpected.add("c3,2");
            docsExpected.add("c4,2");
            docsExpected.add("d3,2");
            docsExpected.add("d4,2");
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobDataTypes() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";

            Set<String> docsExpectedA = new TreeSet<>();
            docsExpectedA.add("a0,0");
            docsExpectedA.add("a2,0");
            docsExpectedA.add("a4,0");
            docsExpectedA.add("a6,0");
            docsExpectedA.add("a8,0");

            Set<String> docsExpectedB = new TreeSet<>();
            docsExpectedB.add("a1,0");
            docsExpectedB.add("a3,0");
            docsExpectedB.add("a5,0");
            docsExpectedB.add("a7,0");
            docsExpectedB.add("a9,0");

            // Boolean true
            Request q1 = new Request("POST", endpoint);
            q1.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_TRUE);
            Response r1 = client.performRequest(q1);
            JsonNode j1 = Json.ORDERED_MAPPER.readTree(r1.getEntity().getContent());
            assertEquals(5, j1.get("hits").get("total").asInt());
            assertEquals(docsExpectedA, getActualIdHits(j1));

            // Boolean true
            Request q1t = new Request("POST", endpoint);
            q1t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_TRUE_TERMS);
            Response r1t = client.performRequest(q1t);
            JsonNode j1t = Json.ORDERED_MAPPER.readTree(r1t.getEntity().getContent());
            assertEquals(5, j1t.get("hits").get("total").asInt());
            assertEquals(docsExpectedA, getActualIdHits(j1t));

            // Boolean false
            Request q2 = new Request("POST", endpoint);
            q2.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_FALSE);
            Response r2 = client.performRequest(q2);
            JsonNode j2 = Json.ORDERED_MAPPER.readTree(r2.getEntity().getContent());
            assertEquals(5, j2.get("hits").get("total").asInt());
            assertEquals(docsExpectedB, getActualIdHits(j2));

            // Boolean false
            Request q2t = new Request("POST", endpoint);
            q2t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_BOOLEAN_FALSE_TERMS);
            Response r2t = client.performRequest(q2t);
            JsonNode j2t = Json.ORDERED_MAPPER.readTree(r2t.getEntity().getContent());
            assertEquals(5, j2t.get("hits").get("total").asInt());
            assertEquals(docsExpectedB, getActualIdHits(j2t));

            // Double positive
            Request q3 = new Request("POST", endpoint);
            q3.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_POSITIVE);
            Response r3 = client.performRequest(q3);
            JsonNode j3 = Json.ORDERED_MAPPER.readTree(r3.getEntity().getContent());
            assertEquals(5, j3.get("hits").get("total").asInt());
            assertEquals(docsExpectedA, getActualIdHits(j3));

            // Double positive
            Request q3t = new Request("POST", endpoint);
            q3t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_POSITIVE_TERMS);
            Response r3t = client.performRequest(q3t);
            JsonNode j3t = Json.ORDERED_MAPPER.readTree(r3t.getEntity().getContent());
            assertEquals(5, j3t.get("hits").get("total").asInt());
            assertEquals(docsExpectedA, getActualIdHits(j3t));

            // Double negative
            Request q4 = new Request("POST", endpoint);
            q4.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_NEGATIVE);
            Response r4 = client.performRequest(q4);
            JsonNode j4 = Json.ORDERED_MAPPER.readTree(r4.getEntity().getContent());
            assertEquals(5, j4.get("hits").get("total").asInt());
            assertEquals(docsExpectedB, getActualIdHits(j4));

            // Double negative
            Request q4t = new Request("POST", endpoint);
            q4t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_DOUBLE_NEGATIVE_TERMS);
            Response r4t = client.performRequest(q4t);
            JsonNode j4t = Json.ORDERED_MAPPER.readTree(r4t.getEntity().getContent());
            assertEquals(5, j4t.get("hits").get("total").asInt());
            assertEquals(docsExpectedB, getActualIdHits(j4t));

            // Float positive
            Request q5 = new Request("POST", endpoint);
            q5.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_POSITIVE);
            Response r5 = client.performRequest(q5);
            JsonNode j5 = Json.ORDERED_MAPPER.readTree(r5.getEntity().getContent());
            assertEquals(5, j5.get("hits").get("total").asInt());
            assertEquals(docsExpectedA, getActualIdHits(j5));

            // Float positive
            Request q5t = new Request("POST", endpoint);
            q5t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_POSITIVE_TERMS);
            Response r5t = client.performRequest(q5t);
            JsonNode j5t = Json.ORDERED_MAPPER.readTree(r5t.getEntity().getContent());
            assertEquals(5, j5t.get("hits").get("total").asInt());
            assertEquals(docsExpectedA, getActualIdHits(j5t));

            // Float negative
            Request q6 = new Request("POST", endpoint);
            q6.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_NEGATIVE);
            Response r6 = client.performRequest(q6);
            JsonNode j6 = Json.ORDERED_MAPPER.readTree(r6.getEntity().getContent());
            assertEquals(5, j6.get("hits").get("total").asInt());
            assertEquals(docsExpectedB, getActualIdHits(j6));

            // Float negative
            Request q6t = new Request("POST", endpoint);
            q6t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_FLOAT_NEGATIVE_TERMS);
            Response r6t = client.performRequest(q6t);
            JsonNode j6t = Json.ORDERED_MAPPER.readTree(r6t.getEntity().getContent());
            assertEquals(5, j6t.get("hits").get("total").asInt());
            assertEquals(docsExpectedB, getActualIdHits(j6t));

            // Integer positive
            Request q7 = new Request("POST", endpoint);
            q7.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_POSITIVE);
            Response r7 = client.performRequest(q7);
            JsonNode j7 = Json.ORDERED_MAPPER.readTree(r7.getEntity().getContent());
            assertEquals(j7.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActualIdHits(j7));

            // Integer positive
            Request q7t = new Request("POST", endpoint);
            q7t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_POSITIVE_TERMS);
            Response r7t = client.performRequest(q7t);
            JsonNode j7t = Json.ORDERED_MAPPER.readTree(r7t.getEntity().getContent());
            assertEquals(j7t.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActualIdHits(j7t));

            // Integer negative
            Request q8 = new Request("POST", endpoint);
            q8.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_NEGATIVE);
            Response r8 = client.performRequest(q8);
            JsonNode j8 = Json.ORDERED_MAPPER.readTree(r8.getEntity().getContent());
            assertEquals(j8.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActualIdHits(j8));

            // Integer negative
            Request q8t = new Request("POST", endpoint);
            q8t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_INTEGER_NEGATIVE_TERMS);
            Response r8t = client.performRequest(q8t);
            JsonNode j8t = Json.ORDERED_MAPPER.readTree(r8t.getEntity().getContent());
            assertEquals(j8t.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActualIdHits(j8t));

            // Long positive
            Request q9 = new Request("POST", endpoint);
            q9.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_POSITIVE);
            Response r9 = client.performRequest(q9);
            JsonNode j9 = Json.ORDERED_MAPPER.readTree(r9.getEntity().getContent());
            assertEquals(j9.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActualIdHits(j9));

            // Long positive
            Request q9t = new Request("POST", endpoint);
            q9t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_POSITIVE_TERMS);
            Response r9t = client.performRequest(q9t);
            JsonNode j9t = Json.ORDERED_MAPPER.readTree(r9t.getEntity().getContent());
            assertEquals(j9t.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActualIdHits(j9t));

            // Long negative
            Request q10 = new Request("POST", endpoint);
            q10.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_NEGATIVE);
            Response r10 = client.performRequest(q10);
            JsonNode j10 = Json.ORDERED_MAPPER.readTree(r10.getEntity().getContent());
            assertEquals(j10.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActualIdHits(j10));

            // Long negative
            Request q10t = new Request("POST", endpoint);
            q10t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_NUMBER_LONG_NEGATIVE_TERMS);
            Response r10t = client.performRequest(q10t);
            JsonNode j10t = Json.ORDERED_MAPPER.readTree(r10t.getEntity().getContent());
            assertEquals(j10t.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActualIdHits(j10t));

            // String A
            Request q11 = new Request("POST", endpoint);
            q11.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_STRING_A);
            Response r11 = client.performRequest(q11);
            JsonNode j11 = Json.ORDERED_MAPPER.readTree(r11.getEntity().getContent());
            assertEquals(j11.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActualIdHits(j11));

            // String A
            Request q11t = new Request("POST", endpoint);
            q11t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_STRING_A_TERMS);
            Response r11t = client.performRequest(q11t);
            JsonNode j11t = Json.ORDERED_MAPPER.readTree(r11t.getEntity().getContent());
            assertEquals(j11t.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActualIdHits(j11t));

            // String B
            Request q12 = new Request("POST", endpoint);
            q12.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_STRING_B);
            Response r12 = client.performRequest(q12);
            JsonNode j12 = Json.ORDERED_MAPPER.readTree(r12.getEntity().getContent());
            assertEquals(j12.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActualIdHits(j12));

            // String B
            Request q12t = new Request("POST", endpoint);
            q12t.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_STRING_B_TERMS);
            Response r12t = client.performRequest(q12t);
            JsonNode j12t = Json.ORDERED_MAPPER.readTree(r12t.getEntity().getContent());
            assertEquals(j12t.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedB, getActualIdHits(j12t));

        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobDataTypesDate() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a?max_hops=2&max_docs_per_query=2";
            Request postResolution = new Request("POST", endpoint);
            postResolution.addParameter("max_hops", "2");
            postResolution.addParameter("max_docs_per_query", "2");
            postResolution.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_DATE);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            /*
            Elasticsearch 7.0.0 - 7.2.0 has a different behavior when querying date ranges.

            To demonstrate, compare this query (below) with the test indices, data, and entity models on Elasticsearch
            versions 6.7.1 and 7.0.0:

            GET zentity_test_index_d/_search
            {
              "query": {
                "bool": {
                  "filter": [
                    {
                      "range": {
                        "type_date": {
                          "gte": "2000-01-01 00:00:01||-2s",
                          "lte": "2000-01-01 00:00:01||+2s",
                          "format": "yyyy-MM-dd HH:mm:ss"
                        }
                      }
                    }
                  ]
                }
              }
            }

            In 7.0.0 the result has a fourth hit ("_id" = "d3") where the "type_date" field is "2000-01-01T00:00:02.500",
            which is a half second greater than the 2s window that was specified in the search.

            We'll allow this behavior in the test, since this is a behavior of Elasticsearch and not zentity.
            */
            Properties props = new Properties();
            props.load(ZentityPlugin.class.getResourceAsStream("/plugin-descriptor.properties"));
            Set<String> dateBugVersions = new TreeSet<>();
            dateBugVersions.add("7.0.0");
            dateBugVersions.add("7.0.1");
            dateBugVersions.add("7.1.0");
            dateBugVersions.add("7.1.1");
            dateBugVersions.add("7.2.0");
            if (dateBugVersions.contains(props.getProperty("elasticsearch.version"))) {
                assertEquals(json.get("hits").get("total").asInt(), 15);
            } else {
                assertEquals(json.get("hits").get("total").asInt(), 13);
            }
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a1,0");
            docsExpected.add("a2,0");
            docsExpected.add("b0,0");
            docsExpected.add("c0,0");
            docsExpected.add("d0,0");
            docsExpected.add("d1,0");
            docsExpected.add("a3,1");
            docsExpected.add("b3,1");
            docsExpected.add("c1,1");
            docsExpected.add("d2,1");
            docsExpected.add("b1,2");
            docsExpected.add("c3,2");
            if (dateBugVersions.contains(props.getProperty("elasticsearch.version"))) {
                docsExpected.add("d3,1");
                docsExpected.add("a4,2");
                docsExpected.add("c4,2");
            } else {
                docsExpected.add("d3,2");
            }
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobDataTypesDateTerm() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a?max_hops=2&max_docs_per_query=2";
            Request postResolution = new Request("POST", endpoint);
            postResolution.addParameter("max_hops", "2");
            postResolution.addParameter("max_docs_per_query", "2");
            postResolution.setEntity(TEST_PAYLOAD_JOB_DATA_TYPES_DATE_TERMS);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            /*
            Elasticsearch 7.0.0 - 7.2.0 has a different behavior when querying date ranges.

            To demonstrate, compare this query (below) with the test indices, data, and entity models on Elasticsearch
            versions 6.7.1 and 7.0.0:

            GET zentity_test_index_d/_search
            {
              "query": {
                "bool": {
                  "filter": [
                    {
                      "range": {
                        "type_date": {
                          "gte": "2000-01-01 00:00:01||-2s",
                          "lte": "2000-01-01 00:00:01||+2s",
                          "format": "yyyy-MM-dd HH:mm:ss"
                        }
                      }
                    }
                  ]
                }
              }
            }

            In 7.0.0 the result has a fourth hit ("_id" = "d3") where the "type_date" field is "2000-01-01T00:00:02.500",
            which is a half second greater than the 2s window that was specified in the search.

            We'll allow this behavior in the test, since this is a behavior of Elasticsearch and not zentity.
            */
            Properties props = new Properties();
            props.load(ZentityPlugin.class.getResourceAsStream("/plugin-descriptor.properties"));
            Set<String> dateBugVersions = new TreeSet<>();
            dateBugVersions.add("7.0.0");
            dateBugVersions.add("7.0.1");
            dateBugVersions.add("7.1.0");
            dateBugVersions.add("7.1.1");
            dateBugVersions.add("7.2.0");
            if (dateBugVersions.contains(props.getProperty("elasticsearch.version"))) {
                assertEquals(json.get("hits").get("total").asInt(), 15);
            } else {
                assertEquals(json.get("hits").get("total").asInt(), 13);
            }
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a1,0");
            docsExpected.add("a2,0");
            docsExpected.add("b0,0");
            docsExpected.add("c0,0");
            docsExpected.add("d0,0");
            docsExpected.add("d1,0");
            docsExpected.add("a3,1");
            docsExpected.add("b3,1");
            docsExpected.add("c1,1");
            docsExpected.add("d2,1");
            docsExpected.add("b1,2");
            docsExpected.add("c3,2");
            if (dateBugVersions.contains(props.getProperty("elasticsearch.version"))) {
                docsExpected.add("d3,1");
                docsExpected.add("a4,2");
                docsExpected.add("c4,2");
            } else {
                docsExpected.add("d3,2");
            }
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobObject() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Set<String> docsExpectedA = new TreeSet<>();
            docsExpectedA.add("a0,0");
            docsExpectedA.add("a2,0");
            docsExpectedA.add("a4,0");
            docsExpectedA.add("a6,0");
            docsExpectedA.add("a8,0");

            // Boolean true
            Request q1 = new Request("POST", endpoint);
            q1.setEntity(TEST_PAYLOAD_JOB_OBJECT);
            Response r1 = client.performRequest(q1);
            JsonNode j1 = Json.ORDERED_MAPPER.readTree(r1.getEntity().getContent());
            assertEquals(j1.get("hits").get("total").asInt(), 5);
            assertEquals(docsExpectedA, getActualIdHits(j1));

        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobScopeExcludeAttributes() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_ATTRIBUTES);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 16);
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("b0,0");
            docsExpected.add("a2,1");
            docsExpected.add("b2,1");
            docsExpected.add("c0,1");
            docsExpected.add("c1,1");
            docsExpected.add("c2,1");
            docsExpected.add("a3,2");
            docsExpected.add("a4,2");
            docsExpected.add("a5,2");
            docsExpected.add("b3,2");
            docsExpected.add("b4,2");
            docsExpected.add("b5,2");
            docsExpected.add("c3,2");
            docsExpected.add("c4,2");
            docsExpected.add("c5,2");
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobScopeExcludeAttributesTerms() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_ATTRIBUTES_TERMS);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 16);
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("b0,0");
            docsExpected.add("a2,1");
            docsExpected.add("b2,1");
            docsExpected.add("c0,1");
            docsExpected.add("c1,1");
            docsExpected.add("c2,1");
            docsExpected.add("a3,2");
            docsExpected.add("a4,2");
            docsExpected.add("a5,2");
            docsExpected.add("b3,2");
            docsExpected.add("b4,2");
            docsExpected.add("b5,2");
            docsExpected.add("c3,2");
            docsExpected.add("c4,2");
            docsExpected.add("c5,2");
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobScopeIncludeAttributes() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_SCOPE_INCLUDE_ATTRIBUTES);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 8);
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("a2,0");
            docsExpected.add("b0,0");
            docsExpected.add("b2,0");
            docsExpected.add("c0,0");
            docsExpected.add("c2,0");
            docsExpected.add("d0,0");
            docsExpected.add("d2,0");
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobScopeIncludeAttributesTerms() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_SCOPE_INCLUDE_ATTRIBUTES_TERMS);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 8);
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("a2,0");
            docsExpected.add("b0,0");
            docsExpected.add("b2,0");
            docsExpected.add("c0,0");
            docsExpected.add("c2,0");
            docsExpected.add("d0,0");
            docsExpected.add("d2,0");
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobScopeExcludeAndIncludeAttributes() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_AND_INCLUDE_ATTRIBUTES);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 4);
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a2,0");
            docsExpected.add("b2,0");
            docsExpected.add("c2,0");
            docsExpected.add("d2,0");
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobScopeExcludeAndIncludeAttributesTerms() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_SCOPE_EXCLUDE_AND_INCLUDE_ATTRIBUTES_TERMS);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 4);
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a2,0");
            docsExpected.add("b2,0");
            docsExpected.add("c2,0");
            docsExpected.add("d2,0");
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobResolverWeight() throws Exception {
        int testResourceSet = TEST_RESOURCES_B;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_b";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_RESOLVER_WEIGHT);
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 4);
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a2,0");
            docsExpected.add("a3,0");
            docsExpected.add("a4,1");
            docsExpected.add("a5,1");
            assertEquals(docsExpected, getActualIdHits(json));
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobElasticsearchError() throws Exception {
        int testResourceSet = TEST_RESOURCES_ELASTICSEARCH_ERROR;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_elasticsearch_error";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_ERROR);
            try {
                client.performRequest(postResolution);
                fail("expected failure");
            } catch (ResponseException e) {
                Response response = e.getResponse();
                assertEquals(response.getStatusLine().getStatusCode(), 500);
                JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

                assertTrue("error object field exists", json.has("error") && json.get("error").isObject());
                JsonNode errorObj = json.get("error");
                assertEquals("elasticsearch", errorObj.get("by").asText());
                assertEquals("org.elasticsearch.common.ParsingException", errorObj.get("type").asText());
                assertFalse(errorObj.get("reason").asText().isEmpty());
                assertFalse(errorObj.get("stack_trace").asText().isEmpty());

                assertTrue("hits object field exists", json.has("hits") && json.get("hits").isObject());
                assertEquals(json.get("hits").get("total").asInt(), 2);

                Set<String> docsExpected = new TreeSet<>();
                docsExpected.add("a2,0");
                docsExpected.add("a3,0");
                assertEquals(docsExpected, getActualIdHits(json));
            }

            // Test error_trace=false and queries=true
            String endpointQueriesNoTrace = "_zentity/resolution/zentity_test_entity_elasticsearch_error";
            Request postResolutionQueriesNoTrace  = new Request("POST", endpointQueriesNoTrace);
            postResolutionQueriesNoTrace.addParameter("error_trace", "true");
            postResolutionQueriesNoTrace.addParameter("queries", "true");
            postResolutionQueriesNoTrace.setEntity(TEST_PAYLOAD_JOB_ERROR);
            try {
                client.performRequest(postResolutionQueriesNoTrace);
                fail("expected failure");
            } catch (ResponseException e) {
                Response response = e.getResponse();
                assertEquals(500, response.getStatusLine().getStatusCode());

                String content = IOUtils.toString(response.getEntity().getContent(), Charset.defaultCharset());
                JsonNode json = Json.ORDERED_MAPPER.readTree(content);

                assertTrue("error object field exists", json.has("error") && json.get("error").isObject());
                JsonNode errorObj = json.get("error");
                assertTrue("has 'by' field", errorObj.has("by") && errorObj.get("by").isTextual());
                assertEquals(errorObj.get("by").asText(), "elasticsearch");
                assertEquals(errorObj.get("type").asText(), "org.elasticsearch.common.ParsingException");
                assertTrue(errorObj.get("reason").asText().contains("example_malformed_query"));
                assertNotNull("Should contain a stack trace", errorObj.get("stack_trace"));

                assertFalse(json.get("queries").isMissingNode());
                assertEquals(2, json.get("hits").get("total").asInt());

                Set<String> docsExpected = new TreeSet<>();
                docsExpected.add("a2,0");
                docsExpected.add("a3,0");
                assertEquals(docsExpected, getActualIdHits(json));
            }
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobZentityError() throws Exception {
        int testResourceSet = TEST_RESOURCES_ZENTITY_ERROR;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_zentity_error";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_ERROR);
            try {
                client.performRequest(postResolution);
                fail("expected failure");
            } catch (ResponseException e) {
                Response response = e.getResponse();
                assertEquals(response.getStatusLine().getStatusCode(), 500);
                JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
                assertTrue("has an error", json.has("error"));

                JsonNode errorJson = json.get("error");
                assertEquals(errorJson.get("by").asText(), "zentity");
                assertEquals(errorJson.get("type").asText(), "io.zentity.model.ValidationException");
                assertEquals(errorJson.get("reason").asText(), "Expected 'number' attribute data type.");
                assertTrue(errorJson.get("stack_trace").asText().startsWith("io.zentity.model.ValidationException: Expected 'number' attribute data type."));

                assertEquals(json.get("hits").get("total").asInt(), 0);
            }

            // Test error_trace=false and queries=true
            String endpointQueriesNoTrace = "_zentity/resolution/zentity_test_entity_zentity_error";
            Request postResolutionQueriesNoTrace  = new Request("POST", endpointQueriesNoTrace);
            postResolutionQueriesNoTrace.addParameter("error_trace", "false");
            postResolutionQueriesNoTrace.addParameter("queries", "true");
            postResolutionQueriesNoTrace.setEntity(TEST_PAYLOAD_JOB_ERROR);
            try {
                client.performRequest(postResolutionQueriesNoTrace);
                fail("expected failure");
            } catch (ResponseException e) {
                Response response = e.getResponse();
                assertEquals(response.getStatusLine().getStatusCode(), 500);
                JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
                assertEquals(json.get("error").get("by").asText(), "zentity");
                assertEquals(json.get("error").get("type").asText(), "io.zentity.model.ValidationException");
                assertEquals(json.get("error").get("reason").asText(), "Expected 'number' attribute data type.");
                assertNull(json.get("error").get("stack_trace"));
                assertFalse(json.get("queries").isMissingNode());
                assertEquals(json.get("hits").get("total").asInt(), 0);
            }
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobArrays() throws Exception {
        int testResourceSet = TEST_RESOURCES_ARRAYS;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_arrays";
            Request req = new Request("POST", endpoint);
            req.addParameter("_explanation", "true");
            req.setEntity(TEST_PAYLOAD_JOB_ARRAYS);
            Response res = client.performRequest(req);
            JsonNode resJson = Json.ORDERED_MAPPER.readTree(res.getEntity().getContent());

            // round-trip json so that it is ordered
            resJson = Json.ORDERED_MAPPER.readTree(Json.ORDERED_MAPPER.writeValueAsString(resJson));

            assertEquals(resJson.get("hits").get("total").asInt(), 2);

            Set<String> docsExpectedArrays = new TreeSet<>();
            docsExpectedArrays.add("1,0");
            docsExpectedArrays.add("2,1");
            assertEquals(docsExpectedArrays, getActualIdHits(resJson));

            for (JsonNode doc : resJson.get("hits").get("hits")) {
                String attributesExpectedJson;
                String explanationExpectedJson;
                String id = doc.get("_id").asText();
                switch (id) {
                    case "1":
                        attributesExpectedJson = "{\"string\":[\"abc\"],\"array\":[\"111\",\"222\",\"333\",\"444\"]}";
                        explanationExpectedJson = "{\"resolvers\":{\"string\":{\"attributes\":[\"string\"]},\"array\":{\"attributes\":[\"array\"]}},\"matches\":[{\"attribute\":\"array\",\"target_field\":\"array_2\",\"target_value\":[\"222\",null,\"222\"],\"input_value\":\"222\",\"input_matcher\":\"exact\",\"input_matcher_params\":{}},{\"attribute\":\"array\",\"target_field\":\"array_4\",\"target_value\":[\"222\",\"333\",\"444\"],\"input_value\":\"222\",\"input_matcher\":\"exact\",\"input_matcher_params\":{}},{\"attribute\":\"string\",\"target_field\":\"string\",\"target_value\":\"abc\",\"input_value\":\"abc\",\"input_matcher\":\"exact\",\"input_matcher_params\":{}}]}";
                        break;
                    case "2":
                        attributesExpectedJson = "{\"string\":[\"xyz\"],\"array\":[\"444\",\"555\"]}";
                        explanationExpectedJson = "{\"resolvers\":{\"array\":{\"attributes\":[\"array\"]}},\"matches\":[{\"attribute\":\"array\",\"target_field\":\"array_1\",\"target_value\":[\"444\"],\"input_value\":\"444\",\"input_matcher\":\"exact\",\"input_matcher_params\":{}}]}";
                        break;
                    default:
                        throw new RuntimeException("Unexpected hit: " + id);
                }
                JsonNode attributesExpected = Json.ORDERED_MAPPER.readTree(attributesExpectedJson);
                assertUnorderedEquals("attributes", attributesExpected, doc.get("_attributes"));

                JsonNode explanationExpected = Json.ORDERED_MAPPER.readTree(explanationExpectedJson);
                assertUnorderedEquals("explanation", explanationExpected, doc.get("_explanation"));
            }

        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobSearchParams() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_ATTRIBUTES);
            postResolution.addParameter("_seq_no_primary_term", "true");
            postResolution.addParameter("_version", "true");
            postResolution.addParameter("max_time_per_query", "5s");
            postResolution.addParameter("search.allow_partial_search_results", "true");
            postResolution.addParameter("search.batched_reduce_size", "5");
            postResolution.addParameter("search.max_concurrent_shard_requests", "5");
            postResolution.addParameter("search.pre_filter_shard_size", "5");
            postResolution.addParameter("search.request_cache", "true");
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());
            assertEquals(json.get("hits").get("total").asInt(), 6);
            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("b0,0");
            docsExpected.add("c0,1");
            docsExpected.add("a1,2");
            docsExpected.add("b1,3");
            docsExpected.add("c1,4");
            assertEquals(docsExpected, getActualIdHits(json));
            for (JsonNode doc : json.get("hits").get("hits")) {
                assertTrue(doc.has("_primary_term"));
                assertTrue(doc.has("_seq_no"));
                assertTrue(doc.has("_version"));
            }

            String endpoint2 = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution2 = new Request("POST", endpoint2);
            postResolution2.setEntity(TEST_PAYLOAD_JOB_ATTRIBUTES);
            postResolution2.addParameter("_seq_no_primary_term", "false");
            postResolution2.addParameter("_version", "false");
            Response response2 = client.performRequest(postResolution2);
            JsonNode json2 = Json.ORDERED_MAPPER.readTree(response2.getEntity().getContent());
            assertEquals(json2.get("hits").get("total").asInt(), 6);
            Set<String> docsExpected2 = new TreeSet<>();
            docsExpected2.add("a0,0");
            docsExpected2.add("b0,0");
            docsExpected2.add("c0,1");
            docsExpected2.add("a1,2");
            docsExpected2.add("b1,3");
            docsExpected2.add("c1,4");
            assertEquals(docsExpected2, getActualIdHits(json2));
            for (JsonNode doc : json2.get("hits").get("hits")) {
                assertFalse(doc.has("_primary_term"));
                assertFalse(doc.has("_seq_no"));
                assertFalse(doc.has("_version"));
            }
        } finally {
            destroyTestResources(testResourceSet);
        }
    }

    @Test
    public void testJobScore() throws Exception {
        int testResourceSet = TEST_RESOURCES_A;
        prepareTestResources(testResourceSet);
        try {
            String endpoint = "_zentity/resolution/zentity_test_entity_a";
            Request postResolution = new Request("POST", endpoint);
            postResolution.setEntity(TEST_PAYLOAD_JOB_NO_SCOPE);
            postResolution.addParameter("_explanation", "true");
            postResolution.addParameter("_score", "true");
            postResolution.addParameter("max_docs_per_query", "1");
            postResolution.addParameter("max_hops", "3");
            Response response = client.performRequest(postResolution);
            JsonNode json = Json.ORDERED_MAPPER.readTree(response.getEntity().getContent());

            Set<String> docsExpected = new TreeSet<>();
            docsExpected.add("a0,0");
            docsExpected.add("b0,0");
            docsExpected.add("a1,1");
            docsExpected.add("b1,1");
            docsExpected.add("c0,1");
            docsExpected.add("d0,1");
            docsExpected.add("a2,2");
            docsExpected.add("b2,2");
            docsExpected.add("c1,2");
            docsExpected.add("d1,2");
            docsExpected.add("a3,3");
            docsExpected.add("b3,3");
            docsExpected.add("c2,3");
            docsExpected.add("d2,3");
            Set<String> docsActual = getActualIdHits(json);
            assertEquals(docsExpected, docsActual);

            assertEquals(json.get("hits").get("total").asInt(), 14);
            for (JsonNode hit : json.get("hits").get("hits")) {
                JsonNode scoreNode = hit.get("_score");
                assertTrue(scoreNode.isFloatingPointNumber() || scoreNode.isNull());
                double score = scoreNode.doubleValue();
                String id = hit.get("_id").textValue();
                switch (id) {
                    case "a0":
                    case "b0":
                        assertEquals(0.794, score, 0.0000000001);
                        break;
                    case "a1":
                    case "b1":
                        assertEquals(0.5, score, 0.0000000001);
                        break;
                    case "c0":
                    case "d0":
                    case "c2":
                    case "d2":
                        assertEquals(0.0, score,  0.0000000001);
                        break;
                    case "a2":
                    case "b2":
                        assertEquals(0.8426393720609059, score, 0.0000000001);
                        break;
                    case "c1":
                        assertEquals(0.9356979368877253, score, 0.0000000001);
                        break;
                    case "d1":
                        assertEquals(0.9262128928820453, score, 0.0000000001);
                        break;
                    case "a3":
                        assertEquals(0.9684567702655289, score, 0.0000000001);
                        break;
                    case "b3":
                        assertEquals(0.9680814702469515, score, 0.0000000001);
                        break;
                    default:
                        Assert.fail("Unexpected matched doc id: " + id);
                }
                for (JsonNode match : hit.get("_explanation").get("matches")) {
                    assertFalse(match.get("score").isMissingNode());
                }
            }
        } finally {
            destroyTestResources(testResourceSet);
        }
    }
}
