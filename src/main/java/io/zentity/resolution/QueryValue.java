package io.zentity.resolution;

import io.zentity.resolution.input.value.Value;

import java.util.Base64;

import static io.zentity.common.Patterns.COLON;

public class QueryValue {
    public String attributeName;
    public String indexFieldName;
    public String matcherName;
    public Integer nameId;
    public Value value;
    public String serializedValue;

    private transient String encodedValue;

    public QueryValue() {
    }

    public QueryValue(
        String attributeName,
        String indexFieldName,
        String matcherName,
        Value value,
        Integer nameId
    ) {
        this.attributeName = attributeName;
        this.indexFieldName = indexFieldName;
        this.matcherName = matcherName;
        this.value = value;
        this.nameId = nameId;
    }

    private static String encodeValue(Value value) {
        return Base64.getEncoder().encodeToString(value.serialized().getBytes());
    }

    private static String decodeValue(String encoded) {
        return new String(Base64.getDecoder().decode(encoded));
    }

    public static QueryValue deserialize(String serialized) {
        String[] nameParts = COLON.split(serialized);
        if (nameParts.length != 5) {
            throw new IllegalArgumentException("Query Values must be in the form attributeName:indexFieldName:matcherName:encodedValue:id. Got: " + serialized);
        }

        QueryValue queryValue = new QueryValue();
        queryValue.attributeName = nameParts[0];
        queryValue.indexFieldName = nameParts[1];
        queryValue.matcherName = nameParts[2];
        // TODO: deserialize this into a full Value object?
        queryValue.serializedValue = decodeValue(nameParts[3]);
        queryValue.nameId = Integer.valueOf(nameParts[4]);

        return queryValue;
    }

    public String serialize() {
        return attributeName + ":" + indexFieldName + ":" + matcherName + ":" + getEncodedValue() + ":" + nameId;
    }

    /**
     * A name that does not include any unique identifiers.
     *
     * @return The generic name.
     */
    public String genericName() {
        return attributeName + ":" + indexFieldName + ":" + matcherName + ":" + serializedValue;
    }

    private String getEncodedValue() {
        if (encodedValue == null) {
            encodedValue = encodeValue(value);
        }

        return encodedValue;
    }
}
