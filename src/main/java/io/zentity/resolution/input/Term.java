package io.zentity.resolution.input;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.zentity.common.Json;
import io.zentity.common.Patterns;
import io.zentity.model.ValidationException;
import io.zentity.resolution.input.value.BooleanValue;
import io.zentity.resolution.input.value.DateValue;
import io.zentity.resolution.input.value.NumberValue;
import io.zentity.resolution.input.value.StringValue;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Objects;

public class Term implements Comparable<Term> {

    private final String term;
    private Boolean isBoolean;
    private Boolean isDate;
    private Boolean isNumber;
    private BooleanValue booleanValue;
    private DateValue dateValue;
    private NumberValue numberValue;
    private StringValue stringValue;

    public Term(String term) throws ValidationException {
        validateTerm(term);
        this.term = term;
    }

    private static boolean isBoolean(String term) {
        String termLowerCase = term.toLowerCase();
        return termLowerCase.equals("true") || termLowerCase.equals("false");
    }

    private static boolean asBoolean(String term) {
        return Boolean.parseBoolean(term);
    }

    private static boolean isDate(String term, String format) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(format);
            formatter.setLenient(false);
            formatter.parse(term);
        } catch (ParseException e) {
            return false;
        }
        return true;
    }

    private static boolean isNumber(String term) {
        return Patterns.NUMBER_STRING.matcher(term).matches();
    }

    private void validateTerm(String term) throws ValidationException {
        Objects.requireNonNull(term, "term cannot be null");
        if (Patterns.EMPTY_STRING.matcher(term).matches()) {
            throw new ValidationException("A term must be a non-empty string.");
        }
    }

    /**
     * Check if the term string is a boolean value.
     * Lazily store the decision and then return the decision.
     *
     * @return
     */
    public boolean isBoolean() {
        if (this.isBoolean == null) {
            this.isBoolean = isBoolean(this.term);
        }
        return this.isBoolean;
    }

    /**
     * Check if the term string is a date value.
     * Lazily store the decision and then return the decision.
     *
     * @return
     */
    public boolean isDate(String format) {
        if (this.isDate == null) {
            this.isDate = isDate(this.term, format);
        }
        return this.isDate;
    }

    /**
     * Convert the term to a BooleanValue.
     * Lazily store the value and then return it.
     *
     * @return
     */
    public BooleanValue booleanValue() throws ValidationException {
        if (this.booleanValue == null) {
            JsonNode valNode = BooleanNode.valueOf(asBoolean(this.term));
            this.booleanValue = new BooleanValue(valNode);
        }
        return this.booleanValue;
    }

    /**
     * Check if the term string is a number value.
     * Lazily store the decision and then return the decision.
     *
     * @return
     */
    public boolean isNumber() {
        if (this.isNumber == null) {
            this.isNumber = isNumber(this.term);
        }
        return this.isNumber;
    }

    /**
     * Convert the term to a DateValue.
     * Lazily store the value and then return it.
     *
     * @return
     */
    public DateValue dateValue() throws ValidationException {
        if (this.dateValue == null) {
            this.dateValue = new DateValue(new TextNode(this.term));
        }
        return this.dateValue;
    }

    /**
     * Convert the term to a NumberValue.
     * Lazily store the value and then return it.
     *
     * @return
     */
    public NumberValue numberValue() throws IOException, ValidationException {
        if (this.numberValue == null) {
            JsonNode value = Json.parseNumberAsNode(this.term);

            this.numberValue = new NumberValue(value);
        }
        return this.numberValue;
    }

    /**
     * Convert the term to a StringValue.
     * Lazily store the value and then return it.
     *
     * @return
     */
    public StringValue stringValue() throws ValidationException {
        if (this.stringValue == null) {
            this.stringValue = new StringValue(TextNode.valueOf(this.term));
        }
        return this.stringValue;
    }

    @Override
    public int compareTo(Term o) {
        return this.term.compareTo(o.term);
    }

    @Override
    public String toString() {
        return this.term;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }

        if (o.getClass() != this.getClass()) {
            return false;
        }

        Term otherTerm = (Term) o;

        return otherTerm.term.equals(term);
    }

    @Override
    public int hashCode() { return this.term.hashCode(); }
}
