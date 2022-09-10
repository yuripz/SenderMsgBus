package ru.hermes.msgbus.ws;

import java.util.ArrayList;
import java.util.List;

/**
 * Default exception thrown by the SoapBuilder.
 *
 * @author Tom Bujok
 * @since 1.0.0
 */
public class SoapValidationException extends SoapException {

    private final List<AssertionError> errors;

    public SoapValidationException(List<AssertionError> errors) {
        super("Message validation failed with " + errors.size() + " error(s)\n" + errors);
        this.errors = errors;
    }

    public List<AssertionError> getErrors() {
        return new ArrayList<AssertionError>(errors);
    }

}
