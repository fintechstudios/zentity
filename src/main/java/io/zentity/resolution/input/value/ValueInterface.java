package io.zentity.resolution.input.value;

import com.fasterxml.jackson.databind.JsonNode;
import io.zentity.model.ValidationException;

// TODO: is both an interface and an abstract class necessary, or can this be merged into the Value class?
public interface ValueInterface extends Comparable<Value> {

    /**
     * Validate the attribute value. Throw an exception on validation error. Pass on success.
     *
     * @param value Attribute value.
     */
    void validate(JsonNode value) throws ValidationException;

    /**
     * Serialize the attribute value from a JsonNode object to a String object.
     */
    String serialize(JsonNode value);

    /**
     * Return the attribute type.
     *
     * @return The type.
     */
    String type();

    /**
     * Return the attribute value.
     *
     * @return The raw value.
     */
    Object value();

    /**
     * Return the serialized attribute value.
     *
     * @return The JSON string.
     */
    String serialized();

}
