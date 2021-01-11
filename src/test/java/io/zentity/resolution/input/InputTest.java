package io.zentity.resolution.input;


import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.common.Json;
import io.zentity.model.IndexTest;
import io.zentity.model.MatcherTest;
import io.zentity.model.Model;
import io.zentity.model.ModelTest;
import io.zentity.model.ResolverTest;
import io.zentity.model.ValidationException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class InputTest {

    // Valid input
    private static final String validAttributeArray = "\"attribute_array\":[\"abc\"]";
    private static final String validAttributeObject = "\"attribute_object\":{\"values\":[\"abc\"]}";
    private static final String validAttributes = "\"attributes\":{" + validAttributeArray + "," + validAttributeObject + "}";
    private static final String validIds = "\"ids\":{\"index_name_a\":[\"a\",\"b\",\"c\"]}";
    private static final String validModel = "\"model\":" + ModelTest.VALID_OBJECT;
    private static final String validAttributesEmpty = "\"attributes\":{}";
    private static final String validAttributesEmptyTypeNull = "\"attributes\":null";
    private static final String validAttributeTypeArray = "\"attributes\":{\"attribute_name\":[\"abc\"]}";
    private static final String validAttributeTypeArrayEmpty = "\"attributes\":{\"attribute_name\":[]}";
    private static final String validAttributeTypeNull = "\"attributes\":{\"attribute_name\":null}";
    private static final String validAttributeTypeBoolean = "\"attributes\":{\"attribute_type_boolean\":[true]}";
    private static final String validAttributeTypeNumber = "\"attributes\":{\"attribute_type_number\":[1.0]}";
    private static final String validAttributeTypeString = "\"attributes\":{\"attribute_type_string\":[\"abc\"]}";
    private static final String validTermsEmpty = "\"terms\":[]";
    private static final String validTermsEmptyTypeNull = "\"terms\":null";
    private static final String validTermsTypeArray = "\"terms\":[\"abc\"]";
    private static final String validIdsEmpty = "\"ids\":{}";
    private static final String validIdsEmptyTypeNull = "\"ids\":null";

    private static final String validScopeAttributesEmpty = "\"attributes\":{}";
    private static final String validScopeAttributesTypeNull = "\"attributes\":null";
    private static final String validScopeAttributeTypeArray = "\"attributes\":{\"attribute_name\":[\"abc\"]}";
    private static final String validScopeAttributeTypeArrayEmpty = "\"attributes\":{\"attribute_name\":[]}";
    private static final String validScopeAttributeTypeNull = "\"attributes\":{\"attribute_name\":null}";
    private static final String validScopeAttributeTypeBoolean = "\"attributes\":{\"attribute_type_boolean\":[true]}";
    private static final String validScopeAttributeTypeNumber = "\"attributes\":{\"attribute_type_number\":[1.0]}";
    private static final String validScopeAttributeTypeString = "\"attributes\":{\"attribute_type_string\":[\"abc\"]}";
    private static final String validScopeIndicesTypeArray = "\"indices\":[\"index_name_a\"]";
    private static final String validScopeIndicesTypeArrayEmpty = "\"indices\":[]";
    private static final String validScopeIndicesTypeNull = "\"indices\":null";
    private static final String validScopeIndicesTypeString = "\"indices\":\"index_name_a\"";
    private static final String validScopeResolversTypeArray = "\"resolvers\":[\"resolver_name_a\"]";
    private static final String validScopeResolversTypeArrayEmpty = "\"resolvers\":[]";
    private static final String validScopeResolversTypeNull = "\"resolvers\":null";
    private static final String validScopeResolversTypeString = "\"resolvers\":\"resolver_name_a\"";
    private static final String validScopeEmpty = "\"scope\":{}";
    private static final String validScopeTypeNull = "\"scope\":null";
    private static final String validScopeExcludeEmpty = "\"scope\":{\"exclude\":{}}";
    private static final String validScopeExcludeTypeNull = "\"scope\":{\"exclude\":null}";
    private static final String validScopeIncludeEmpty = "\"scope\":{\"include\":{}}";
    private static final String validScopeIncludeTypeNull = "\"scope\":{\"include\":null}";
    private static final String validInput = "{" + validAttributes + "," + validModel + "}";

    // Invalid attributes
    private static final String invalidAttributesEmpty = "\"attributes\":{}";
    private static final String invalidAttributesTypeArray = "\"attributes\":[]";
    private static final String invalidAttributesTypeFloat = "\"attributes\":1.0";
    private static final String invalidAttributesTypeInteger = "\"attributes\":1";
    private static final String invalidAttributesTypeNull = "\"attributes\":null";
    private static final String invalidAttributesTypeString = "\"attributes\":\"abc\"";
    private static final String invalidAttributeNotFoundArray = "\"attributes\":{\"attribute_name_x\":[\"abc\"]}";
    private static final String invalidAttributeNotFoundObject = "\"attributes\":{\"attribute_name_x\":{\"values\":[\"abc\"]}}";

    // Invalid terms
    private static final String invalidTermsTypeArray = "\"terms\":{}";
    private static final String invalidTermsTypeFloat = "\"terms\":1.0";
    private static final String invalidTermsTypeInteger = "\"terms\":1";
    private static final String invalidTermsTypeString = "\"terms\":\"abc\"";

    // Invalid ids
    private static final String invalidIdsTypeArray = "\"ids\":[]";
    private static final String invalidIdsTypeFloat = "\"ids\":1.0";
    private static final String invalidIdsTypeInteger = "\"ids\":1";
    private static final String invalidIdsTypeString = "\"ids\":\"abc\"";
    private static final String invalidIdsIndexNotFound = "\"ids\":{\"index_name_x\":[\"a\",\"b\",\"c\"]}";
    private static final String invalidIdsIndexTypeFloat = "\"ids\":1.0";
    private static final String invalidIdsIndexTypeInteger = "\"ids\":1";
    private static final String invalidIdsIndexTypeObject = "\"ids\":{}";
    private static final String invalidIdsIndexTypeString = "\"ids\":\"abc\"";
    private static final String invalidIdsIndexIdEmpty = "\"ids\":{\"index_name_a\":[\" \",\"b\",\"c\"]}";
    private static final String invalidIdsIndexIdTypeArray = "\"ids\":{\"index_name_a\":[[],\"b\",\"c\"]}";
    private static final String invalidIdsIndexIdTypeFloat = "\"ids\":{\"index_name_a\":[1.0,\"b\",\"c\"]}";
    private static final String invalidIdsIndexIdTypeInteger = "\"ids\":{\"index_name_a\":[1,\"b\",\"c\"]}";
    private static final String invalidIdsIndexIdTypeNull = "\"ids\":{\"index_name_a\":[null,\"b\",\"c\"]}";
    private static final String invalidIdsIndexIdTypeObject = "\"ids\":{\"index_name_a\":[{},\"b\",\"c\"]}";

    // Invalid model
    private static final String invalidModelEmpty = "\"model\":{}";
    private static final String invalidModelTypeArray = "\"model\":[]";
    private static final String invalidModelTypeFloat = "\"model\":1.0";
    private static final String invalidModelTypeInteger = "\"model\":1";
    private static final String invalidModelTypeNull = "\"model\":null";
    private static final String invalidModelTypeString = "\"model\":\"abc\"";

    // Invalid scope
    private static final String invalidScopeUnrecognizedField = "\"scope\":{\"foo\":{}}";
    private static final String invalidScopeTypeArray = "\"scope\":[]";
    private static final String invalidScopeTypeFloat = "\"scope\":1.0";
    private static final String invalidScopeTypeInteger = "\"scope\":1";
    private static final String invalidScopeTypeString = "\"scope\":\"abc\"";
    private static final String invalidScopeExcludeTypeArray = "\"scope\":{\"exclude\":[]}";
    private static final String invalidScopeExcludeTypeBoolean = "\"scope\":{\"exclude\":true}";
    private static final String invalidScopeExcludeTypeFloat = "\"scope\":{\"exclude\":1.0}";
    private static final String invalidScopeExcludeTypeInteger = "\"scope\":{\"exclude\":1}";
    private static final String invalidScopeExcludeTypeString = "\"scope\":{\"exclude\":\"abc\"}";
    private static final String invalidScopeExcludeUnrecognizedField = "\"scope\":{\"exclude\":{\"foo\":{}}}";
    private static final String invalidScopeIncludeTypeArray = "\"scope\":{\"include\":[]}";
    private static final String invalidScopeIncludeTypeBoolean = "\"scope\":{\"include\":true}";
    private static final String invalidScopeIncludeTypeFloat = "\"scope\":{\"include\":1.0}";
    private static final String invalidScopeIncludeTypeInteger = "\"scope\":{\"include\":1}";
    private static final String invalidScopeIncludeTypeString = "\"scope\":{\"include\":\"abc\"}";
    private static final String invalidScopeIncludeUnrecognizedField = "\"scope\":{\"include\":{\"foo\":{}}}";

    // Invalid scope.attributes
    private static final String invalidScopeAttributeNotFoundArray = "\"attributes\":{\"attribute_name_x\":[\"abc\"]}";
    private static final String invalidScopeAttributeNotFoundString = "\"attributes\":{\"attribute_name_x\":\"abc\"}";
    private static final String invalidScopeAttributesTypeArray = "\"attributes\":[]";
    private static final String invalidScopeAttributesTypeBoolean = "\"attributes\":true";
    private static final String invalidScopeAttributesTypeFloat = "\"attributes\":1.0";
    private static final String invalidScopeAttributesTypeInteger = "\"attributes\":1";
    private static final String invalidScopeAttributesTypeString = "\"attributes\":\"abc\"";

    // Invalid scope.indices
    private static final String invalidScopeIndicesNotFoundArray = "\"indices\":[\"index_name_not_found\"]";
    private static final String invalidScopeIndicesNotFoundString = "\"indices\":\"index_name_not_found\"";
    private static final String invalidScopeIndicesTypeArrayFloat = "\"indices\":[1.0]";
    private static final String invalidScopeIndicesTypeArrayInteger = "\"indices\":[1]";
    private static final String invalidScopeIndicesTypeArrayObject = "\"indices\":[{\"abc\":\"xyz\"}]";
    private static final String invalidScopeIndicesTypeArrayNull = "\"indices\":[null]";
    private static final String invalidScopeIndicesTypeArrayStringEmpty = "\"indices\":[\"\"]";
    private static final String invalidScopeIndicesTypeFloat = "\"indices\":1.0";
    private static final String invalidScopeIndicesTypeInteger = "\"indices\":1";
    private static final String invalidScopeIndicesTypeObject = "\"indices\":{\"abc\":\"xyz\"}";

    // Invalid scope.resolvers
    private static final String invalidScopeResolversNotFoundArray = "\"resolvers\":[\"resolver_name_not_found\"]";
    private static final String invalidScopeResolversNotFoundString = "\"resolvers\":\"resolver_name_not_found\"";
    private static final String invalidScopeResolversTypeArrayFloat = "\"resolvers\":[1.0]";
    private static final String invalidScopeResolversTypeArrayInteger = "\"resolvers\":[1]";
    private static final String invalidScopeResolversTypeArrayObject = "\"resolvers\":[{\"abc\":\"xyz\"}]";
    private static final String invalidScopeResolversTypeArrayNull = "\"resolvers\":[null]";
    private static final String invalidScopeResolversTypeArrayStringEmpty = "\"resolvers\":[\"\"]";
    private static final String invalidScopeResolversTypeFloat = "\"resolvers\":1.0";
    private static final String invalidScopeResolversTypeInteger = "\"resolvers\":1";
    private static final String invalidScopeResolversTypeObject = "\"resolvers\":{\"abc\":\"xyz\"}";

    // Attribute types
    private static final String validAttributeTypes = "\"attributes\":{\"attribute_type_string\":[\"abc\"],\"attribute_type_number\":[1.0],\"attribute_type_boolean\":[true]}";
    private static final String validAttributeTypesModel = "\"model\": {\n" +
        "  \"attributes\":{\"attribute_type_string\":{\"type\":\"string\"},\"attribute_type_number\":{\"type\":\"number\"},\"attribute_type_boolean\":{\"type\":\"boolean\"}},\n" +
        "  \"resolvers\":{\"resolver_name_a\":" + ResolverTest.VALID_OBJECT + "},\n" +
        "  \"matchers\":{\"matcher_name\":" + MatcherTest.VALID_OBJECT + "},\n" +
        "  \"indices\":{\"index_name_a\":" + IndexTest.VALID_OBJECT + "}\n" +
        "}";

    private static String inputAttributes(String attributes) {
        return "{" + attributes + "," + validModel + "}";
    }

    private static String inputIds(String ids) {
        return "{" + ids + "," + validModel + "}";
    }

    private static String inputModel(String model) {
        return "{" + validAttributes + "," + model + "}";
    }

    private static String inputModelAttributeTypes(String attributes) {
        return "{" + attributes + "," + validAttributeTypesModel + "}";
    }

    private static String inputModelAttributeTypesScope(String scope) {
        return "{" + validAttributeTypes + "," + validAttributeTypesModel + "," + scope + "}";
    }

    private static String inputScope(String scope) {
        return "{" + validAttributes + "," + validModel + "," + scope + "}";
    }

    private static String inputScopeExcludeAttributes(String scopeAttributes) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"exclude\":{" + scopeAttributes + "}}}";
    }

    private static String inputScopeExcludeIndices(String scopeIndices) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"exclude\":{" + scopeIndices + "}}}";
    }

    private static String inputScopeExcludeResolvers(String scopeResolvers) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"exclude\":{" + scopeResolvers + "}}}";
    }

    private static String inputScopeIncludeAttributes(String scopeAttributes) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"include\":{" + scopeAttributes + "}}}";
    }

    private static String inputScopeIncludeIndices(String scopeIndices) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"include\":{" + scopeIndices + "}}}";
    }

    private static String inputScopeIncludeResolvers(String scopeResolvers) {
        return "{" + validAttributes + "," + validModel + ",\"scope\":{\"include\":{" + scopeResolvers + "}}}";
    }

    private static JsonNode parseJson(String mock) throws IOException {
        return Json.MAPPER.readTree(mock);
    }

    private static void parseInput(String json, Model model) throws IOException, ValidationException {
        new Input(json, model);
    }

    private static void parseInput(String json) throws IOException, ValidationException {
        new Input(json);
    }

    @Test
    public void testValidInput() throws Exception {
        parseInput(validInput);
    }

    @Test
    public void testValidInputAttributesMissing() throws Exception {
        parseInput("{" + validIds + "}", new Model(ModelTest.VALID_OBJECT));
    }

    @Test
    public void testValidInputAttributesEmpty() throws Exception {
        parseInput("{" + validAttributesEmpty + "," + validIds + "}", new Model(ModelTest.VALID_OBJECT));
        parseInput("{" + validAttributesEmptyTypeNull + "," + validIds + "}", new Model(ModelTest.VALID_OBJECT));
    }

    @Test
    public void testValidInputTermsEmpty() throws Exception {
        parseInput("{" + validAttributes + "," + validTermsEmpty + "}", new Model(ModelTest.VALID_OBJECT));
        parseInput("{" + validAttributes + "," + validTermsEmptyTypeNull + "}", new Model(ModelTest.VALID_OBJECT));
    }

    @Test
    public void testValidInputTermsArray() throws Exception {
        parseInput("{" + validTermsTypeArray+ "}", new Model(ModelTest.VALID_OBJECT));
    }

    @Test
    public void testValidInputIdsMissing() throws Exception {
        parseInput("{" + validAttributes + "}", new Model(ModelTest.VALID_OBJECT));
    }

    @Test
    public void testValidInputIdsEmpty() throws Exception {
        parseInput("{" + validAttributes + "," + validIdsEmpty + "}", new Model(ModelTest.VALID_OBJECT));
        parseInput("{" + validAttributes + "," + validIdsEmptyTypeNull + "}", new Model(ModelTest.VALID_OBJECT));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidInputAttributesEmptyIdsEmpty() throws Exception {
        parseInput("{" + validAttributesEmpty + "," + validIdsEmpty + "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidInputAttributesEmptyIdsEmptyTypeNull() throws Exception {
        parseInput("{" + validAttributesEmpty + "," + validIdsEmptyTypeNull + "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidInputAttributesEmptyTypeNullIdsEmpty() throws Exception {
        parseInput("{" + validAttributesEmptyTypeNull + "," + validIdsEmpty + "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidInputAttributesEmptyTypeNullIdsEmptyTypeNull() throws Exception {
        parseInput("{" + validAttributesEmptyTypeNull + "," + validIdsEmptyTypeNull + "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidInputAttributesEmptyIdsMissing() throws Exception {
        parseInput("{" + validAttributesEmpty + "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidInputAttributesEmptyTypeNullIdsMissing() throws Exception {
        parseInput("{" + validAttributesEmptyTypeNull + "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidInputAttributesMissingIdsEmpty() throws Exception {
        parseInput("{" + validIdsEmpty + "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidInputAttributesMissingIdsEmptyTypeNull() throws Exception {
        parseInput("{" + validIdsEmptyTypeNull + "}");
    }

    @Test(expected = ValidationException.class)
    public void testInvalidInputAttributesMissingIdsMissing() throws Exception {
        parseInput("{}");
    }

    ////  "attributes"  ////////////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesEmpty() throws Exception {
        new Input(inputAttributes(invalidAttributesEmpty));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeArray() throws Exception {
        new Input(inputAttributes(invalidAttributesTypeArray));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeFloat() throws Exception {
        new Input(inputAttributes(invalidAttributesTypeFloat));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeInteger() throws Exception {
        new Input(inputAttributes(invalidAttributesTypeInteger));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeNull() throws Exception {
        new Input(inputAttributes(invalidAttributesTypeNull));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributesTypeString() throws Exception {
        new Input(inputAttributes(invalidAttributesTypeString));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeNotFoundArray() throws Exception {
        String input = inputAttributes(invalidAttributeNotFoundArray);
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeNotFoundObject() throws Exception {
        String input = inputAttributes(invalidAttributeNotFoundObject);
        new Input(input);
    }

    @Test
    public void testValidAttributeTypeArray() throws Exception {
        String input = inputAttributes(validAttributeTypeArray);
        new Input(input);
    }

    @Test
    public void testValidAttributeTypeArrayEmpty() throws Exception {
        String input = inputAttributes(validAttributeTypeArrayEmpty);
        new Input(input);
    }

    @Test
    public void testValidAttributeTypeNull() throws Exception {
        String input = inputAttributes(validAttributeTypeNull);
        new Input(input);
    }

    @Test
    public void testValidAttributeTypeBoolean() throws Exception {
        String input = inputModelAttributeTypes(validAttributeTypeBoolean);
        new Input(input);
    }

    @Test
    public void testValidAttributeTypeNumber() throws Exception {
        String input = inputModelAttributeTypes(validAttributeTypeNumber);
        new Input(input);
    }

    @Test
    public void testValidAttributeTypeString() throws Exception {
        String input = inputModelAttributeTypes(validAttributeTypeString);
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeBooleanWhenNumber() throws Exception {
        String input = inputModelAttributeTypes("\"attributes\":{\"attribute_type_boolean\":[1.0]}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeBooleanWhenString() throws Exception {
        String input = inputModelAttributeTypes("\"attributes\":{\"attribute_type_boolean\":[\"abc\"]}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeNumberWhenBoolean() throws Exception {
        String input = inputModelAttributeTypes("\"attributes\":{\"attribute_type_number\":[true]}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeNumberWhenString() throws Exception {
        String input = inputModelAttributeTypes("\"attributes\":{\"attribute_type_number\":[\"abc\"]}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeStringWhenBoolean() throws Exception {
        String input = inputModelAttributeTypes("\"attributes\":{\"attribute_type_string\":[true]}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidAttributeTypeStringWhenNumber() throws Exception {
        String input = inputModelAttributeTypes("\"attributes\":{\"attribute_type_string\":[1.0]}");
        new Input(input);
    }

    ////  "terms"  ////////////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidTermsTypeArray() throws Exception {
        new Input(inputAttributes(invalidTermsTypeArray));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTermsTypeFloat() throws Exception {
        new Input(inputAttributes(invalidTermsTypeFloat));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTermsTypeInteger() throws Exception {
        new Input(inputAttributes(invalidTermsTypeInteger));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidTermsTypeString() throws Exception {
        new Input(inputAttributes(invalidTermsTypeString));
    }

    ////  "ids"  ///////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidIds() throws Exception {
        new Input(inputIds(validIds));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsTypeArray() throws Exception {
        new Input(inputIds(invalidIdsTypeArray));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsTypeFloat() throws Exception {
        new Input(inputIds(invalidIdsTypeFloat));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsTypeInteger() throws Exception {
        new Input(inputIds(invalidIdsTypeInteger));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsTypeString() throws Exception {
        new Input(inputIds(invalidIdsTypeString));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsIndexNotFound() throws Exception {
        new Input(inputIds(invalidIdsIndexNotFound));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsIndexTypeFloat() throws Exception {
        new Input(inputIds(invalidIdsIndexTypeFloat));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsIndexTypeInteger() throws Exception {
        new Input(inputIds(invalidIdsIndexTypeInteger));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsIndexTypeObject() throws Exception {
        new Input(inputIds(invalidIdsIndexTypeObject));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsIndexTypeString() throws Exception {
        new Input(inputIds(invalidIdsIndexTypeString));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsIndexIdEmpty() throws Exception {
        new Input(inputIds(invalidIdsIndexIdEmpty));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsIndexIdTypeArray() throws Exception {
        new Input(inputIds(invalidIdsIndexIdTypeArray));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsIndexIdTypeFloat() throws Exception {
        new Input(inputIds(invalidIdsIndexIdTypeFloat));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsIndexIdTypeInteger() throws Exception {
        new Input(inputIds(invalidIdsIndexIdTypeInteger));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsIndexIdTypeNull() throws Exception {
        new Input(inputIds(invalidIdsIndexIdTypeNull));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidIdsIndexIdTypeObject() throws Exception {
        new Input(inputIds(invalidIdsIndexIdTypeObject));
    }

    ////  "model"  /////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test(expected = ValidationException.class)
    public void testInvalidModelEmpty() throws Exception {
        new Model(parseJson(inputModel(invalidModelEmpty)).get("model"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidModelTypeArray() throws Exception {
        new Model(parseJson(inputModel(invalidModelTypeArray)).get("model"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidModelTypeFloat() throws Exception {
        new Model(parseJson(inputModel(invalidModelTypeFloat)).get("model"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidModelTypeInteger() throws Exception {
        new Model(parseJson(inputModel(invalidModelTypeInteger)).get("model"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidModelTypeNull() throws Exception {
        new Model(parseJson(inputModel(invalidModelTypeNull)).get("model"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidModelTypeString() throws Exception {
        new Model(parseJson(inputModel(invalidModelTypeString)).get("model"));
    }

    ////  "scope"  /////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeEmpty() throws Exception {
        new Input(inputScope(validScopeEmpty));
    }

    @Test
    public void testValidScopeTypeNull() throws Exception {
        new Input(inputScope(validScopeTypeNull));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeTypeArray() throws Exception {
        new Input(inputScope(invalidScopeTypeArray));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeTypeFloat() throws Exception {
        new Input(inputScope(invalidScopeTypeFloat));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeTypeInteger() throws Exception {
        new Input(inputScope(invalidScopeTypeInteger));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeTypeString() throws Exception {
        new Input(inputScope(invalidScopeTypeString));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeUnrecognizedField() throws Exception {
        new Input(inputScope(invalidScopeUnrecognizedField), new Model(ModelTest.VALID_OBJECT));
    }

    ////  "scope"."exclude"  ///////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeExcludeEmpty() throws Exception {
        new Input(inputScope(validScopeExcludeEmpty));
    }

    @Test
    public void testValidScopeExcludeTypeNull() throws Exception {
        new Input(inputScope(validScopeExcludeTypeNull));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeTypeArray() throws Exception {
        new Input(inputScope(invalidScopeExcludeTypeArray));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeTypeBoolean() throws Exception {
        new Input(inputScope(invalidScopeExcludeTypeBoolean));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeTypeFloat() throws Exception {
        new Input(inputScope(invalidScopeExcludeTypeFloat));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeTypeInteger() throws Exception {
        new Input(inputScope(invalidScopeExcludeTypeInteger));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeTypeString() throws Exception {
        new Input(inputScope(invalidScopeExcludeTypeString));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeUnrecognizedField() throws Exception {
        new Input(inputScope(invalidScopeExcludeUnrecognizedField));
    }

    ////  "scope"."include"  ///////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeIncludeEmpty() throws Exception {
        new Input(inputScope(validScopeIncludeEmpty));
    }

    @Test
    public void testValidScopeIncludeTypeNull() throws Exception {
        new Input(inputScope(validScopeIncludeTypeNull));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeTypeArray() throws Exception {
        new Input(inputScope(invalidScopeIncludeTypeArray));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeTypeBoolean() throws Exception {
        new Input(inputScope(invalidScopeIncludeTypeBoolean));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeTypeFloat() throws Exception {
        new Input(inputScope(invalidScopeIncludeTypeFloat));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeTypeInteger() throws Exception {
        new Input(inputScope(invalidScopeIncludeTypeInteger));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeTypeString() throws Exception {
        new Input(inputScope(invalidScopeIncludeTypeString));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeUnrecognizedField() throws Exception {
        new Input(inputScope(invalidScopeIncludeUnrecognizedField));
    }

    ////  "scope"."exclude"."attributes"  //////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeExcludeAttributesEmpty() throws Exception {
        new Input(inputScopeExcludeAttributes(validScopeAttributesEmpty));
    }

    @Test
    public void testValidScopeExcludeAttributesTypeNull() throws Exception {
        new Input(inputScopeExcludeAttributes(validScopeAttributesTypeNull));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributesTypeArray() throws Exception {
        new Input(inputScopeExcludeAttributes(invalidScopeAttributesTypeArray));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributesTypeBoolean() throws Exception {
        new Input(inputScopeExcludeAttributes(invalidScopeAttributesTypeBoolean));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributesTypeFloat() throws Exception {
        new Input(inputScopeExcludeAttributes(invalidScopeAttributesTypeFloat));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributesTypeInteger() throws Exception {
        new Input(inputScopeExcludeAttributes(invalidScopeAttributesTypeInteger));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributesTypeString() throws Exception {
        new Input(inputScopeExcludeAttributes(invalidScopeAttributesTypeString));
    }

    ////  "scope"."include"."attributes"  //////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeIncludeAttributesEmpty() throws Exception {
        new Input(inputScopeIncludeAttributes(validScopeAttributesEmpty));
    }

    @Test
    public void testValidScopeIncludeAttributesTypeNull() throws Exception {
        new Input(inputScopeIncludeAttributes(validScopeAttributesTypeNull));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributesTypeArray() throws Exception {
        new Input(inputScopeIncludeAttributes(invalidScopeAttributesTypeArray));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributesTypeBoolean() throws Exception {
        new Input(inputScopeIncludeAttributes(invalidScopeAttributesTypeBoolean));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributesTypeFloat() throws Exception {
        new Input(inputScopeIncludeAttributes(invalidScopeAttributesTypeFloat));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributesTypeInteger() throws Exception {
        new Input(inputScopeIncludeAttributes(invalidScopeAttributesTypeInteger));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributesTypeString() throws Exception {
        new Input(inputScopeIncludeAttributes(invalidScopeAttributesTypeString));
    }

    ////  "scope"."exclude"."attributes".ATTRIBUTE_NAME  ///////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeExcludeAttributeTypeArray() throws Exception {
        new Input(inputScopeExcludeAttributes(validScopeAttributeTypeArray));
    }

    @Test
    public void testValidScopeExcludeAttributeTypeArrayEmpty() throws Exception {
        new Input(inputScopeExcludeAttributes(validScopeAttributeTypeArrayEmpty));
    }

    @Test
    public void testValidScopeExcludeAttributeTypeNull() throws Exception {
        new Input(inputScopeExcludeAttributes(validScopeAttributeTypeNull));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeNotFoundArray() throws Exception {
        String input = inputScopeExcludeAttributes(invalidScopeAttributeNotFoundArray);
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeNotFoundString() throws Exception {
        String input = inputScopeExcludeAttributes(invalidScopeAttributeNotFoundString);
        new Input(input);
    }

    @Test
    public void testValidScopeExcludeAttributeTypeBoolean() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"exclude\":{" + validScopeAttributeTypeBoolean + "}}");
        new Input(input);
    }

    @Test
    public void testValidScopeExcludeAttributeTypeNumber() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"exclude\":{" + validScopeAttributeTypeNumber + "}}");
        new Input(input);
    }

    @Test
    public void testValidScopeExcludeAttributeTypeString() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"exclude\":{" + validScopeAttributeTypeString + "}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeBooleanWhenNumber() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_boolean\":[1.0]}}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeBooleanWhenString() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_boolean\":[\"abc\"]}}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeNumberWhenBoolean() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_number\":[true]}}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeNumberWhenString() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_number\":[\"abc\"]}}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeStringWhenBoolean() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_string\":[true]}}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeAttributeTypeStringWhenNumber() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"exclude\":{\"attributes\":{\"attribute_type_string\":[1.0]}}}");
        new Input(input);
    }

    ////  "scope"."include"."attributes".ATTRIBUTE_NAME  ///////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeIncludeAttributeTypeArray() throws Exception {
        new Input(inputScopeIncludeAttributes(validScopeAttributeTypeArray));
    }

    @Test
    public void testValidScopeIncludeAttributeTypeArrayEmpty() throws Exception {
        new Input(inputScopeIncludeAttributes(validScopeAttributeTypeArrayEmpty));
    }

    @Test
    public void testValidScopeIncludeAttributeTypeNull() throws Exception {
        new Input(inputScopeIncludeAttributes(validScopeAttributeTypeNull));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeNotFoundArray() throws Exception {
        String input = inputScopeIncludeAttributes(invalidScopeAttributeNotFoundArray);
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeNotFoundString() throws Exception {
        String input = inputScopeIncludeAttributes(invalidScopeAttributeNotFoundString);
        new Input(input);
    }

    @Test
    public void testValidScopeIncludeAttributeTypeBoolean() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"include\":{" + validScopeAttributeTypeBoolean + "}}");
        new Input(input);
    }

    @Test
    public void testValidScopeIncludeAttributeTypeNumber() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"include\":{" + validScopeAttributeTypeNumber + "}}");
        new Input(input);
    }

    @Test
    public void testValidScopeIncludeAttributeTypeString() throws Exception {
        String input = inputModelAttributeTypesScope("\"scope\":{\"include\":{" + validScopeAttributeTypeString + "}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeBooleanWhenNumber() throws Exception {
        String input = inputScopeIncludeAttributes("\"scope\":{\"include\":{\"attributes\":{\"attribute_type_boolean\":[1.0]}}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeBooleanWhenString() throws Exception {
        String input = inputScopeIncludeAttributes("\"scope\":{\"include\":{\"attributes\":{\"attribute_type_boolean\":[\"abc\"]}}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeNumberWhenBoolean() throws Exception {
        String input = inputScopeIncludeAttributes("\"scope\":{\"include\":{\"attributes\":{\"attribute_type_number\":[true]}}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeNumberWhenString() throws Exception {
        String input = inputScopeIncludeAttributes("\"scope\":{\"include\":{\"attributes\":{\"attribute_type_number\":[\"abc\"]}}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeStringWhenBoolean() throws Exception {
        String input = inputScopeIncludeAttributes("\"scope\":{\"include\":{\"attributes\":{\"attribute_type_string\":[true]}}}");
        new Input(input);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeAttributeTypeStringWhenNumber() throws Exception {
        String input = inputScopeIncludeAttributes("\"scope\":{\"include\":{\"attributes\":{\"attribute_type_string\":[1.0]}}}");
        new Input(input);
    }

    ////  "scope"."exclude"."indices"  /////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeExcludeIndicesTypeArray() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(validScopeIndicesTypeArray));
        Input input = new Input(json);
        Assert.assertFalse(input.model().indices().containsKey("index_name_a"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_b"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeExcludeIndicesTypeArrayEmpty() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(validScopeIndicesTypeArrayEmpty));
        Input input = new Input(json);
        Assert.assertTrue(input.model().indices().containsKey("index_name_a"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_b"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeExcludeIndicesTypeNull() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(validScopeIndicesTypeNull));
        Input input = new Input(json);
        Assert.assertTrue(input.model().indices().containsKey("index_name_a"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_b"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeExcludeIndicesTypeString() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(validScopeIndicesTypeString));
        Input input = new Input(json);
        Assert.assertFalse(input.model().indices().containsKey("index_name_a"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_b"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_c"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeIndicesNotFoundArray() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(invalidScopeIndicesNotFoundArray));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeIndicesNotFoundString() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(invalidScopeIndicesNotFoundString));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeIndicesTypeArrayFloat() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(invalidScopeIndicesTypeArrayFloat));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeIndicesTypeArrayInteger() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(invalidScopeIndicesTypeArrayInteger));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeIndicesTypeArrayNull() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(invalidScopeIndicesTypeArrayNull));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeIndicesTypeArrayObject() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(invalidScopeIndicesTypeArrayObject));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeIndicesTypeArrayStringEmpty() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(invalidScopeIndicesTypeArrayStringEmpty));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeIndicesTypeFloat() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(invalidScopeIndicesTypeFloat));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeIndicesTypeInteger() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(invalidScopeIndicesTypeInteger));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeIndicesTypeObject() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeIndices(invalidScopeIndicesTypeObject));
        new Input(json);
    }

    ////  "scope"."include"."indices"  /////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeIncludeIndicesTypeArray() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(validScopeIndicesTypeArray));
        Input input = new Input(json);
        Assert.assertTrue(input.model().indices().containsKey("index_name_a"));
        Assert.assertFalse(input.model().indices().containsKey("index_name_b"));
        Assert.assertFalse(input.model().indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeIncludeIndicesTypeArrayEmpty() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(validScopeIndicesTypeArrayEmpty));
        Input input = new Input(json);
        Assert.assertTrue(input.model().indices().containsKey("index_name_a"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_b"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeIncludeIndicesTypeNull() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(validScopeIndicesTypeNull));
        Input input = new Input(json);
        Assert.assertTrue(input.model().indices().containsKey("index_name_a"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_b"));
        Assert.assertTrue(input.model().indices().containsKey("index_name_c"));
    }

    @Test
    public void testValidScopeIncludeIndicesTypeString() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(validScopeIndicesTypeString));
        Input input = new Input(json);
        Assert.assertTrue(input.model().indices().containsKey("index_name_a"));
        Assert.assertFalse(input.model().indices().containsKey("index_name_b"));
        Assert.assertFalse(input.model().indices().containsKey("index_name_c"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeIndicesNotFoundArray() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(invalidScopeIndicesNotFoundArray));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeIndicesNotFoundString() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(invalidScopeIndicesNotFoundString));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeIndicesTypeArrayFloat() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(invalidScopeIndicesTypeArrayFloat));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeIndicesTypeArrayInteger() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(invalidScopeIndicesTypeArrayInteger));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeIndicesTypeArrayObject() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(invalidScopeIndicesTypeArrayObject));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeIndicesTypeArrayNull() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(invalidScopeIndicesTypeArrayNull));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeIndicesTypeArrayStringEmpty() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(invalidScopeIndicesTypeArrayStringEmpty));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeIndicesTypeFloat() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(invalidScopeIndicesTypeFloat));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeIndicesTypeInteger() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(invalidScopeIndicesTypeInteger));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeIndicesTypeObject() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeIndices(invalidScopeIndicesTypeObject));
        new Input(json);
    }

    ////  "scope"."exclude"."resolvers"  ///////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeExcludeResolversTypeArray() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(validScopeResolversTypeArray));
        Input input = new Input(json);
        Assert.assertFalse(input.model().resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeExcludeResolversTypeArrayEmpty() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(validScopeResolversTypeArrayEmpty));
        Input input = new Input(json);
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeExcludeResolversTypeNull() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(validScopeResolversTypeNull));
        Input input = new Input(json);
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeExcludeResolversTypeString() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(validScopeResolversTypeString));
        Input input = new Input(json);
        Assert.assertFalse(input.model().resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_c"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeResolversNotFoundArray() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(invalidScopeResolversNotFoundArray));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeResolversNotFoundString() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(invalidScopeResolversNotFoundString));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeResolversTypeArrayFloat() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(invalidScopeResolversTypeArrayFloat));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeResolversTypeArrayInteger() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(invalidScopeResolversTypeArrayInteger));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeResolversTypeArrayObject() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(invalidScopeResolversTypeArrayObject));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeResolversTypeArrayNull() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(invalidScopeResolversTypeArrayNull));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeResolversTypeArrayStringEmpty() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(invalidScopeResolversTypeArrayStringEmpty));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeResolversTypeFloat() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(invalidScopeResolversTypeFloat));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeResolversTypeInteger() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(invalidScopeResolversTypeInteger));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeExcludeResolversTypeObject() throws Exception {
        JsonNode json = parseJson(inputScopeExcludeResolvers(invalidScopeResolversTypeObject));
        new Input(json);
    }

    ////  "scope"."include"."resolvers"  ///////////////////////////////////////////////////////////////////////////////

    @Test
    public void testValidScopeIncludeResolversTypeArray() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(validScopeResolversTypeArray));
        Input input = new Input(json);
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_a"));
        Assert.assertFalse(input.model().resolvers().containsKey("resolver_name_b"));
        Assert.assertFalse(input.model().resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeIncludeResolversTypeArrayEmpty() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(validScopeResolversTypeArrayEmpty));
        Input input = new Input(json);
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeIncludeResolversTypeNull() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(validScopeResolversTypeNull));
        Input input = new Input(json);
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_a"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_b"));
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_c"));
    }

    @Test
    public void testValidScopeIncludeResolversTypeString() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(validScopeResolversTypeString));
        Input input = new Input(json);
        Assert.assertTrue(input.model().resolvers().containsKey("resolver_name_a"));
        Assert.assertFalse(input.model().resolvers().containsKey("resolver_name_b"));
        Assert.assertFalse(input.model().resolvers().containsKey("resolver_name_c"));
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeResolversNotFoundArray() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(invalidScopeResolversNotFoundArray));
        new Input(json);

    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeResolversNotFoundString() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(invalidScopeResolversNotFoundString));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeResolversTypeArrayFloat() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(invalidScopeResolversTypeArrayFloat));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeResolversTypeArrayInteger() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(invalidScopeResolversTypeArrayInteger));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeResolversTypeArrayObject() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(invalidScopeResolversTypeArrayObject));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeResolversTypeArrayNull() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(invalidScopeResolversTypeArrayNull));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeResolversTypeArrayStringEmpty() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(invalidScopeResolversTypeArrayStringEmpty));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeResolversTypeFloat() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(invalidScopeResolversTypeFloat));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeResolversTypeInteger() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(invalidScopeResolversTypeInteger));
        new Input(json);
    }

    @Test(expected = ValidationException.class)
    public void testInvalidScopeIncludeResolversTypeObject() throws Exception {
        JsonNode json = parseJson(inputScopeIncludeResolvers(invalidScopeResolversTypeObject));
        new Input(json);
    }

}

