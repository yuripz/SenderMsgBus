package net.plumbing.msgbus.init;
import net.sf.saxon.expr.XPathContext;
import net.sf.saxon.lib.ExtensionFunctionCall;
import net.sf.saxon.lib.ExtensionFunctionDefinition;
import net.sf.saxon.ma.map.MapType;
import net.sf.saxon.om.Sequence;
import net.sf.saxon.om.StructuredQName;
import net.sf.saxon.trans.XPathException;
import net.sf.saxon.type.Type;
import net.sf.saxon.value.SequenceType;
import net.sf.saxon.value.StringValue;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import java.io.IOException;

public class MsgBusSaxonJavaExtensions {

    public static class MyConcatenateFunction extends ExtensionFunctionDefinition {

        private static final StructuredQName funcName =
                new StructuredQName("msgbus", "http://msgbus.net/ext", "concatenate");


        @Override
        public StructuredQName getFunctionQName() {
            return funcName;
        }

        @Override
        public SequenceType[] getArgumentTypes() {
            return new SequenceType[]{
                    SequenceType.SINGLE_STRING,  // Argument 1:  string
                    SequenceType.SINGLE_STRING   // Argument 2:  string
            };
        }

        @Override
        public SequenceType getResultType(SequenceType[] suppliedArgumentTypes) {
            return SequenceType.SINGLE_STRING;  // Return type: string
        }

        @Override
        public ExtensionFunctionCall makeCallExpression() {
            return new ExtensionFunctionCall() {
                @Override
                public Sequence call(XPathContext context, Sequence[] arguments) throws XPathException {
                    String arg1 = arguments[0].head().getStringValue();
                    String arg2 = arguments[1].head().getStringValue();
                    return StringValue.makeStringValue(arg1 + arg2);
                }
            };
        }
    }

    public static class Base64ToStringFunction extends ExtensionFunctionDefinition {
        private static final StructuredQName funcName =
                new StructuredQName("msgbus", "http://msgbus.net/ext", "base64string");

        @Override
        public StructuredQName getFunctionQName() {
            return funcName;
        }

        @Override
        public SequenceType[] getArgumentTypes() {
            return new SequenceType[]{
                    SequenceType.SINGLE_STRING // Argument 1: string
            };
        }

        @Override
        public SequenceType getResultType(SequenceType[] suppliedArgumentTypes) {
            return SequenceType.SINGLE_STRING; // Return type: string
        }

        @Override
        public ExtensionFunctionCall makeCallExpression() {
            return new ExtensionFunctionCall() {
                @Override
                public Sequence call(XPathContext context, Sequence[] arguments) throws XPathException {
                    String arg1 = arguments[0].head().getStringValue();
                    try {
                        byte[] decodedBytes  = Base64.getDecoder().decode(arg1);
                        return StringValue.makeStringValue( new String(decodedBytes, StandardCharsets.UTF_8) );
                        //return new String(encodedBytes, StandardCharsets.UTF_8);
                    } catch (Exception e) {
                        System.err.println("Ошибка при кодировании из Base64: " + e.getMessage());
                        return StringValue.makeStringValue(""); // Или выбросить исключение, чтобы обработать ошибку выше
                    }

                }
            };
        }
    }

    public static class StringToBase64Function extends ExtensionFunctionDefinition {
        private static final StructuredQName funcName =
                new StructuredQName("msgbus", "http://msgbus.net/ext", "string2base64");

        @Override
        public StructuredQName getFunctionQName() {
            return funcName;
        }

        @Override
        public SequenceType[] getArgumentTypes() {
            return new SequenceType[]{
                    SequenceType.SINGLE_STRING // Argument 1: string
            };
        }

        @Override
        public SequenceType getResultType(SequenceType[] suppliedArgumentTypes) {
            return SequenceType.SINGLE_STRING; // Return type: string
        }

        @Override
        public ExtensionFunctionCall makeCallExpression() {
            return new ExtensionFunctionCall() {
                @Override
                public Sequence call(XPathContext context, Sequence[] arguments) throws XPathException {
                    String arg1 = arguments[0].head().getStringValue();
                    try {
                        byte[] encodedBytes = Base64.getEncoder().encode(arg1.getBytes(StandardCharsets.UTF_8));
                        return StringValue.makeStringValue( new String(encodedBytes, StandardCharsets.UTF_8) );
                        //return new String(encodedBytes, StandardCharsets.UTF_8);
                    } catch (Exception e) {
                        System.err.println("Ошибка при кодировании в Base64: " + e.getMessage());
                        return StringValue.makeStringValue(""); // Или выбросить исключение, чтобы обработать ошибку выше
                    }

                }
            };

        }
    }

    public static class GetSystemPropertyFunction extends ExtensionFunctionDefinition {

        private static final StructuredQName funcName =
                new StructuredQName("msgbus", "http://msgbus.net/ext", "get-system-property");

        @Override
        public StructuredQName getFunctionQName() {
            return funcName;
        }

        @Override
        public SequenceType[] getArgumentTypes() {
            return new SequenceType[]{
                    SequenceType.SINGLE_STRING // Argument 1: string
            };
        }

        @Override
        public SequenceType getResultType(SequenceType[] suppliedArgumentTypes) {
            return SequenceType.SINGLE_STRING; // Return type: string
        }

        @Override
        public ExtensionFunctionCall makeCallExpression() {
            return new ExtensionFunctionCall() {
                @Override
                public Sequence call(XPathContext context, Sequence[] arguments) throws XPathException {
                    String propertyName = arguments[0].head().getStringValue();
                    String propertyValue = System.getProperty(propertyName);
                    if (propertyValue == null) {
                        return StringValue.makeStringValue("");
                    }
                    return StringValue.makeStringValue(propertyValue);
                }
            };
        }
    }
}
