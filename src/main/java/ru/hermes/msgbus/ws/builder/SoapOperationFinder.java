package ru.hermes.msgbus.ws.builder;

import ru.hermes.msgbus.ws.SoapContext;

/**
 * @author Tom Bujok
 * @since 1.0.0
 */
public interface SoapOperationFinder {

    SoapOperationFinder name(String operationName);

    SoapOperationFinder soapAction(String soapAction);

    SoapOperationFinder inputName(String inputName);

    SoapOperationFinder outputName(String inputName);

    SoapOperationBuilder find();

    SoapOperationBuilder find(SoapContext context);


}
