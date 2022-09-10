package ru.hermes.msgbus.ws.builder.core;

import ru.hermes.msgbus.ws.builder.SoapBuilder;
import ru.hermes.msgbus.ws.builder.SoapOperationBuilder;

import javax.wsdl.Binding;
import javax.wsdl.BindingOperation;
import javax.wsdl.extensions.ExtensibilityElement;
import javax.wsdl.extensions.soap.SOAPOperation;
import javax.wsdl.extensions.soap12.SOAP12Operation;
import java.util.List;

/**
 * @author Tom Bujok
 * @since 1.0.0
 */
public class SoapUtils {

    // removes "" from soap action
    public static String normalizeSoapAction(String soapAction) {
        String normalizedSoapAction = "";
        if (soapAction != null && soapAction.length() > 0) {
            normalizedSoapAction = soapAction;
            if (soapAction.charAt(0) == '"' && soapAction.charAt(soapAction.length() - 1) == '"') {
                normalizedSoapAction = soapAction.substring(1, soapAction.length() - 1).trim();
            }
        }
        return normalizedSoapAction;
    }

    public static String getSOAPActionUri(BindingOperation operation) {
        List extensions = operation.getExtensibilityElements();
        if (extensions != null) {
            for (int i = 0; i < extensions.size(); i++) {
                ExtensibilityElement extElement = (ExtensibilityElement) extensions.get(i);
                if (extElement instanceof SOAPOperation) {
                    SOAPOperation soapOp = (SOAPOperation) extElement;
                    return soapOp.getSoapActionURI();
                } else if (extElement instanceof SOAP12Operation) {
                    SOAP12Operation soapOp = (SOAP12Operation) extElement;
                    return soapOp.getSoapActionURI();
                }
            }
        }
        return null;
    }

    public static SoapOperationBuilder createOperation(SoapBuilder builder, Binding binding, BindingOperation operation, String soapAction) {
        return SoapOperationImpl.create(builder, binding, operation, soapAction);
    }
}
