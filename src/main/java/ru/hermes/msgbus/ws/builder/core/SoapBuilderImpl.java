package ru.hermes.msgbus.ws.builder.core;

import ru.hermes.msgbus.ws.SoapBuilderException;
import ru.hermes.msgbus.ws.SoapContext;
import ru.hermes.msgbus.ws.builder.SoapBuilder;
import ru.hermes.msgbus.ws.builder.SoapOperation;
import ru.hermes.msgbus.ws.builder.SoapOperationBuilder;
import ru.hermes.msgbus.ws.builder.SoapOperationFinder;
import ru.hermes.msgbus.ws.legacy.SoapLegacyFacade;

import javax.wsdl.Binding;
import javax.wsdl.BindingOperation;
import javax.wsdl.Port;
import javax.wsdl.Service;
import javax.xml.namespace.QName;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Tom Bujok
 * @since 1.0.0
 */
class SoapBuilderImpl implements SoapBuilder {

    private final SoapLegacyFacade soapFacade;
    private final Binding binding;
    private final SoapContext context;
    private final List<String> serviceUrls;

    SoapBuilderImpl(SoapLegacyFacade soapFacade, Binding binding, SoapContext context) {
        this.soapFacade = soapFacade;
        this.binding = binding;
        this.context = context;
        this.serviceUrls = new ArrayList<String>();
        initializeServiceUrls();
    }

    @SuppressWarnings("unchecked")
    private void initializeServiceUrls() {
        for (Service service : soapFacade.getServices()) {
            for (Port port : (Collection<Port>) service.getPorts().values()) {
                String address = SoapLegacyFacade.getSoapEndpoint(port);
                if (address != null) {
                    serviceUrls.add(address);
                }
            }
        }
    }

    public BindingOperation getBindingOperation(SoapOperation op) {
        BindingOperation operation = binding.getBindingOperation(op.getOperationName(),
                op.getOperationInputName(), op.getOperationOutputName());
        if (operation == null) {
            throw new SoapBuilderException("Operation not found");
        }
        return operation;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<SoapOperation> getOperations() {
        List<SoapOperation> operationNames = new ArrayList<SoapOperation>();
        for (BindingOperation operation : (List<BindingOperation>) binding.getBindingOperations()) {
            operationNames.add(SoapOperationImpl.create(this, binding, operation));
        }
        return operationNames;
    }

    @Override
    public SoapContext getContext() {
        return context;
    }

    @Override
    public SoapOperationBuilder getOperationBuilder(SoapOperation operation) {
        BindingOperation bindingOperation = getBindingOperation(operation);
        return SoapOperationImpl.create(this, binding, bindingOperation);
    }

    @Override
    public SoapOperationFinder operation() {
        return new SoapOperationFinderImpl(this, binding);
    }

    @Override
    public String buildInputMessage(SoapOperation operation) {
        return buildInputMessage(operation, context);
    }

    @Override
    public String buildInputMessage(SoapOperation operation, SoapContext context) {
        try {
            return soapFacade.buildSoapMessageFromInput(binding, getBindingOperation(operation), context);
        } catch (Exception e) {
            throw new SoapBuilderException(e);
        }
    }

    @Override
    public String buildOutputMessage(SoapOperation operation) {
        return buildOutputMessage(operation, context);
    }

    @Override
    public String buildOutputMessage(SoapOperation operation, SoapContext context) {
        try {
            return soapFacade.buildSoapMessageFromOutput(binding, getBindingOperation(operation), context);
        } catch (Exception e) {
            throw new SoapBuilderException(e);
        }
    }

    @Override
    public String buildFault(String code, String message) {
        return soapFacade.buildFault(code, message, binding, context);
    }

    @Override
    public String buildFault(String code, String message, SoapContext context) {
        return soapFacade.buildFault(code, message, binding, context);
    }

    @Override
    public String buildEmptyFault() {
        return soapFacade.buildEmptyFault(binding, context);
    }

    @Override
    public String buildEmptyFault(SoapContext context) {
        return soapFacade.buildEmptyFault(binding, context);
    }

    @Override
    public String buildEmptyMessage() {
        return soapFacade.buildEmptyMessage(binding, context);
    }

    @Override
    public String buildEmptyMessage(SoapContext context) {
        return soapFacade.buildEmptyMessage(binding, context);
    }

    @Override
    public QName getBindingName() {
        return binding.getQName();
    }

    @Override
    public Binding getBinding() {
        return binding;
    }

    @Override
    public List<String> getServiceUrls() {
        return new ArrayList<String>(serviceUrls);
    }

    @Override
    public void validateInputMessage(SoapOperation operation, String message) {
        BindingOperation bindingOperation = getBindingOperation(operation);
        soapFacade.validateSoapRequestMessage(binding, bindingOperation, message, false);
    }

    @Override
    public void validateInputMessage(SoapOperation operation, String message, boolean strict) {
        BindingOperation bindingOperation = getBindingOperation(operation);
        soapFacade.validateSoapRequestMessage(binding, bindingOperation, message, strict);
    }

    @Override
    public void validateOutputMessage(SoapOperation operation, String message) {
        BindingOperation bindingOperation = getBindingOperation(operation);
        soapFacade.validateSoapResponseMessage(binding, bindingOperation, message, false);
    }

    @Override
    public void validateOutputMessage(SoapOperation operation, String message, boolean strict) {
        BindingOperation bindingOperation = getBindingOperation(operation);
        soapFacade.validateSoapResponseMessage(binding, bindingOperation, message, strict);
    }

    @Override
    public boolean isRpc() {
        return SoapLegacyFacade.isRpc(binding);
    }

    @Override
    public boolean isInputSoapEncoded(SoapOperation operation) {
        return soapFacade.isInputSoapEncoded(getBindingOperation(operation));
    }

    @Override
    public boolean isOutputSoapEncoded(SoapOperation operation) {
        return soapFacade.isOutputSoapEncoded(getBindingOperation(operation));
    }

}
