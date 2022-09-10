package ru.hermes.msgbus.ws.builder;

import ru.hermes.msgbus.ws.SoapContext;

import javax.wsdl.Binding;
import javax.xml.namespace.QName;
import java.util.List;

/**
 * @author Tom Bujok
 * @since 1.0.0
 */
public interface SoapBuilder {

    List<SoapOperation> getOperations();

    SoapOperationFinder operation();

    SoapContext getContext();

    SoapOperationBuilder getOperationBuilder(SoapOperation operation);

    String buildInputMessage(SoapOperation operation);

    String buildInputMessage(SoapOperation operation, SoapContext context);

    String buildOutputMessage(SoapOperation operation);

    String buildOutputMessage(SoapOperation operation, SoapContext context);

    String buildFault(String code, String message);

    String buildFault(String code, String message, SoapContext context);

    String buildEmptyFault();

    String buildEmptyFault(SoapContext context);

    String buildEmptyMessage();

    String buildEmptyMessage(SoapContext context);

    QName getBindingName();

    Binding getBinding();

    List<String> getServiceUrls();

    void validateInputMessage(SoapOperation operation, String message);

    void validateInputMessage(SoapOperation operation, String message, boolean strict);

    void validateOutputMessage(SoapOperation operation, String message);

    void validateOutputMessage(SoapOperation operation, String message, boolean strict);

    boolean isRpc();

    boolean isInputSoapEncoded(SoapOperation operation);

    boolean isOutputSoapEncoded(SoapOperation operation);

}

