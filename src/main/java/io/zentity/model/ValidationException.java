package io.zentity.model;

// TODO: does this need to be a checked exception?
public class ValidationException extends Exception {
    public ValidationException(String message) {
        super(message);
    }
}
