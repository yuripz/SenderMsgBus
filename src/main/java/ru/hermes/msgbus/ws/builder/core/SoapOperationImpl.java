package ru.hermes.msgbus.ws.builder.core;

import ru.hermes.msgbus.ws.SoapContext;
import ru.hermes.msgbus.ws.builder.SoapBuilder;
import ru.hermes.msgbus.ws.builder.SoapOperation;
import ru.hermes.msgbus.ws.builder.SoapOperationBuilder;

import javax.wsdl.Binding;
import javax.wsdl.BindingOperation;
import javax.xml.namespace.QName;

/**
 * @author Tom Bujok
 * @since 1.0.0
 */
class SoapOperationImpl implements SoapOperation, SoapOperationBuilder {

    private final QName bindingName;
    private final String operationName;
    private final String operationInputName;
    private final String operationOutputName;
    private final String soapAction;
    private final SoapBuilder builder;
    private SoapContext context;

    SoapOperationImpl(SoapBuilder builder, QName bindingName, String operationName, String operationInputName,
                      String operationOutputName, String soapAction) {

        this.builder = builder;
        this.bindingName = bindingName;
        this.operationName = operationName;
        this.operationInputName = operationInputName;
        this.operationOutputName = operationOutputName;
        this.soapAction = soapAction;
        this.context = builder.getContext();
    }

    public QName getBindingName() {
        return bindingName;
    }

    public String getOperationName() {
        return operationName;
    }

    public String getOperationInputName() {
        return operationInputName;
    }

    public String getOperationOutputName() {
        return operationOutputName;
    }

    public String getSoapAction() {
        return soapAction;
    }

    @Override
    public boolean isRpc() {
        return builder.isRpc();
    }

    @Override
    public boolean isInputSoapEncoded() {
        return builder.isInputSoapEncoded(this);
    }

    @Override
    public boolean isOutputSoapEncoded() {
        return builder.isOutputSoapEncoded(this);
    }

    static SoapOperationBuilder create(SoapBuilder builder, Binding binding, BindingOperation operation) {
        String soapAction = SoapUtils.getSOAPActionUri(operation);
        return create(builder, binding, operation, soapAction);
    }

    static SoapOperationBuilder create(SoapBuilder builder, Binding binding, BindingOperation operation, String soapAction) {
        String bindingInputName = operation.getBindingInput() != null ? operation.getBindingInput().getName() : null;
        String bindingOutputName = operation.getBindingOutput() != null ? operation.getBindingOutput().getName() : null;
        return new SoapOperationImpl(builder, binding.getQName(), operation.getName(), bindingInputName, bindingOutputName,
                SoapUtils.normalizeSoapAction(soapAction));
    }

    public String toString() {
        return String.format("bindingName=[%s] operationName=[%s] operationInputName=[%s] operationOutputName=[%s] soapAction=[%s]",
                bindingName.toString(), operationName, operationInputName, operationOutputName, soapAction);
    }

    @Override
    public void setContext(SoapContext context) {
        this.context = context;
    }

    @Override
    public SoapContext getContext() {
        return builder.getContext();
    }

    @Override
    public String buildInputMessage() {
        return builder.buildInputMessage(this, context);
    }

    @Override
    public String buildInputMessage(SoapContext context) {
        return builder.buildInputMessage(this, context);
    }

    @Override
    public String buildOutputMessage() {
        return builder.buildOutputMessage(this, context);
    }

    @Override
    public String buildOutputMessage(SoapContext context) {
        return builder.buildOutputMessage(this, context);
    }

    @Override
    public String buildFault(String code, String message) {
        return builder.buildFault(code, message, context);
    }

    @Override
    public String buildFault(String code, String message, SoapContext context) {
        return builder.buildFault(code, message, context);
    }

    @Override
    public String buildEmptyFault() {
        return builder.buildEmptyFault(context);
    }

    @Override
    public String buildEmptyFault(SoapContext context) {
        return builder.buildEmptyFault(context);
    }

    @Override
    public String buildEmptyMessage() {
        return builder.buildEmptyMessage(context);
    }

    @Override
    public String buildEmptyMessage(SoapContext context) {
        return builder.buildEmptyMessage(context);
    }

    @Override
    public void validateInputMessage(String message) {
        builder.validateInputMessage(this, message);
    }

    @Override
    public void validateInputMessage(String message, boolean strict) {
        builder.validateInputMessage(this, message, strict);
    }

    @Override
    public void validateOutputMessage(String message) {
        builder.validateOutputMessage(this, message);
    }

    @Override
    public void validateOutputMessage(String message, boolean strict) {
        builder.validateOutputMessage(this, message, strict);
    }

}
