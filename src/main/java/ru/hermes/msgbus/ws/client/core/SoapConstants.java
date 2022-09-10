package ru.hermes.msgbus.ws.client.core;

/**
 * @author Tom Bujok
 * @since 1.0.0
 */
final public class SoapConstants {

    private SoapConstants() {
    }

    public final static String
            HTTP = "HTTP",
            HTTPS = "HTTPS";

    public final static String
            GET = "GET",
            POST = "POST",
            HEAD = "HEAD",
            PUT = "PUT",
            OPTIONS = "OPTIONS",
            DELETE = "DELETE";

    public final static String
            MIMETYPE_TEXT_HTML = "text/html",
            MIMETYPE_TEXT_PLAIN = "text/plain",
            MIMETYPE_TEXT_XML = "text/xml",
            MIMETYPE_APPLICATION_XML = "application/soap+xml";

    public final static String
            PROP_SOAP_ACTION_11 = "SOAPAction",
            PROP_SOAP_ACTION_12 = "action=",
            PROP_CONTENT_TYPE = "Content-Type",
            PROP_CONTENT_LENGTH = "Content-Length",
            PROP_AUTH = "Authorization",
            PROP_PROXY_AUTH = "Proxy-Authorization",
            PROP_PROXY_CONN = "Proxy-Connection",
            PROP_KEEP_ALIVE = "Keep-Alive",
            PROP_BASIC_AUTH = "Basic",
            PROP_DELIMITER = "; ";


    public final static String
            SOAP_1_1_NAMESPACE = "http://schemas.xmlsoap.org/soap/envelope/",
            SOAP_1_2_NAMESPACE = "http://www.w3.org/2003/05/soap-envelope";

    public final static String
            SSL_CONTEXT_PROTOCOL = "SSLv3",
            TLS_CONTEXT = "TLS",
            JKS_KEYSTORE = "JKS";

    public final static int
            INFINITE_TIMEOUT = 0;

    public static enum AuthMethod {
        NONE, BASIC, DIGEST, NTLM, SPNEGO;
    }

}

