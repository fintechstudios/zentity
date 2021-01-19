package io.zentity.resolution.input.value;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.model.ValidationException;

import java.util.Objects;

public abstract class Value implements ValueInterface {

    protected final String type = "value";
    protected final JsonNode value;
    private String serialized;

    /**
     * Validate and hold the object of a value.
     *
     * @param value Attribute value.
     */
    Value(JsonNode value) throws ValidationException {
        Objects.requireNonNull(value, "Value node cannot be null");
        this.validate(value);
        this.value = value;
    }

    /**
     * Factory method to construct a Value.
     *
     * @param attributeType Attribute type.
     * @param value         Attribute value.
     * @return
     * @throws ValidationException
     */
    public static Value create(String attributeType, JsonNode value) throws ValidationException {
        switch (attributeType) {
            case "boolean":
                return new BooleanValue(value);
            case "date":
                return new DateValue(value);
            case "number":
                return new NumberValue(value);
            case "string":
                return new StringValue(value);
            default:
                throw new ValidationException("'" + attributeType + " is not a recognized attribute type.");
        }
    }

    @Override
    public abstract String serialize(JsonNode value);

    @Override
    public abstract void validate(JsonNode value) throws ValidationException;

    @Override
    public String type() {
        return this.type;
    }

    @Override
    public JsonNode value() {
        return this.value;
    }

    @Override
    public String serialized() {
        // lazy instantiation
        if (this.serialized == null) {
            this.serialized = this.serialize(value);
        }
        return this.serialized;
    }

    @Override
    public int compareTo(Value o) {
        return this.serialized().compareTo(o.serialized());
    }

    @Override
    public String toString() {
        return this.serialized();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (o.getClass() != this.getClass()) {
            return false;
        }

        Value otherVal = (Value) o;

        return serialized().equals(otherVal.serialized());
    }

    @Override
    public int hashCode() {
        return this.serialized().hashCode();
    }
}
