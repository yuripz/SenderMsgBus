package ru.hermes.msgbus.ws.builder;

import javax.xml.namespace.QName;

/**
 * Wrapper object that represents one operation from a WSDL's binding
 *
 * @author Tom Bujok
 * @since 1.0.0
 */
public interface SoapOperation {

    QName getBindingName();

    String getOperationName();

    String getOperationInputName();

    String getOperationOutputName();

    String getSoapAction();

    boolean isRpc();

    boolean isInputSoapEncoded();

    boolean isOutputSoapEncoded();

}

