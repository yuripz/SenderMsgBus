package ru.hermes.msgbus.ws.client;

/**
 * Exception thrown when an transmission error occurs. Returns HTTP error code if returned by the server.
 *
 * @author Tom Bujok
 * @since 1.0.0
 */
public class TransmissionException extends SoapClientException {

    private final String errorResponse;
    private final Integer errorCode;

    public TransmissionException(String message) {
        super(message);
        this.errorResponse = null;
        this.errorCode = null;
    }

    public TransmissionException(String message, Throwable ex) {
        super(String.format("%s; %s", message, ex.getMessage()), ex);
        this.errorResponse = null;
        this.errorCode = null;
    }

    public TransmissionException(String response, Integer errorCode) {
        super(String.format("HTTP response=[%s] code=[%d]", response, errorCode));
        this.errorResponse = response;
        this.errorCode = errorCode;
    }

    public String getErrorResponse() {
        return errorResponse;
    }

    public Integer getErrorCode() {
        return errorCode;
    }
}
