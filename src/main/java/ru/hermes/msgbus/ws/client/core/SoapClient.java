package ru.hermes.msgbus.ws.client.core;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;

import org.apache.http.*;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.NTCredentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.HttpRoutePlanner;
import org.apache.http.conn.routing.RouteInfo;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeLayeredSocketFactory;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import ru.hermes.msgbus.ws.SoapException;
import ru.hermes.msgbus.ws.annotation.ThreadSafe;
import ru.hermes.msgbus.ws.client.SoapClientException;
import ru.hermes.msgbus.ws.client.TransmissionException;
import ru.hermes.msgbus.ws.client.ssl.SSLUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static ru.hermes.msgbus.ws.client.core.SoapConstants.*;

/**
 * SOAP client enables the user to communicate with a SOAP server on a purely XML level.
 * It supports SSL/TLS, basic-authentication and java.net.Proxy.
 * When it comes to SOAP it supports version 1.1 and 1.2 - SOAPAction attribute is automatically properly placed,
 * either in the header (SOAP 1.1) or in the content (SOAP 1.2).
 * SOAP version recognition is based on the SOAP namespace included in the payload.

 *
 * @author Tom Bujok
 * @since 1.0.0
 */
@ThreadSafe
public final class SoapClient {

    // private final static Log log = LogFactory.getLog(SoapClient.class);

    private final static String NULL_SOAP_ACTION = null;

    private int readTimeoutInMillis;
    private int connectTimeoutInMillis;

    private URI endpointUri;
    private Security endpointProperties;
    private boolean endpointTlsEnabled;

    private URI proxyUri;
    private Security proxyProperties;
    private boolean proxyTlsEnabled;

    private DefaultHttpClient client;


    // ----------------------------------------------------------------
    // PUBLIC API
    // ----------------------------------------------------------------

    /**
     * Post the SOAP message to the SOAP server without specifying the SOAPAction
     *
     * @param requestEnvelope SOAP message envelope
     * @return The result returned by the SOAP server
     */
    public String post(String requestEnvelope,   Logger log) {
       // log.info("!!SoapClient.post: "+ requestEnvelope);
        return post(NULL_SOAP_ACTION, requestEnvelope, log);
    }

    /**
     * Post the SOAP message to the SOAP server specifying the SOAPAction
     *
     * @param soapAction      SOAPAction attribute
     * @param requestEnvelope SOAP message envelope
     * @return The result returned by the SOAP server
     */
    public String post(String soapAction, String requestEnvelope, Logger log) {
        log.info(String.format("Sending request to host=[%s] action=[%s] request:%n%s", endpointUri.toString(),
                soapAction, requestEnvelope));
        String response = transmit(soapAction, requestEnvelope, log);
        log.info("Received response:\n" + requestEnvelope);
        return response;
    }

    /**
     * Disconnects from the SOAP server
     * Underlying connection is a persistent connection by default:
     *
     * @link http://docs.oracle.com/javase/1.5.0/docs/guide/net/http-keepalive.html
     */
    public void disconnect() {
        if (client != null) {
            client.getConnectionManager().shutdown();
        }
    }

    // ----------------------------------------------------------------
    // TRANSMISSION API
    // ----------------------------------------------------------------
    private HttpPost generatePost(String soapAction, String requestEnvelope) {
        try {
            HttpPost post = new HttpPost(endpointUri.toString());
            StringEntity contentEntity = new StringEntity(requestEnvelope, StandardCharsets.UTF_8);
            post.setEntity(contentEntity);
            if (requestEnvelope.contains(SOAP_1_1_NAMESPACE)) {
                soapAction = soapAction != null ? "\"" + soapAction + "\"" : "";
                if ( soapAction != null ) // если soapAction пустой то заголовок не заполняем.
                post.addHeader(PROP_SOAP_ACTION_11, soapAction);
                post.addHeader(PROP_CONTENT_TYPE, MIMETYPE_TEXT_XML);
                client.getParams().setParameter(PROP_CONTENT_TYPE, MIMETYPE_TEXT_XML);
            } else if (requestEnvelope.contains(SOAP_1_2_NAMESPACE)) {
                String contentType = MIMETYPE_APPLICATION_XML;
                if (soapAction != null) {
                    contentType = contentType + PROP_DELIMITER + PROP_SOAP_ACTION_12 + "\"" + soapAction + "\"";
                }
                post.addHeader(PROP_CONTENT_TYPE, contentType);
            }
            return post;
        } catch (Exception ex) { //(UnsupportedEncodingException ex) {
            throw new SoapClientException(ex);
        }
    }

    private String transmit(String soapAction, String dataEnvelope, Logger log) {
        HttpPost post = generatePost(soapAction, dataEnvelope);
        return executePost(post, log);
    }

    private String executePost(HttpPost post, Logger log) {
        try {
            log.info("!!! SoapClient.executePost:" + post.getEntity().toString() );
            HttpResponse response = client.execute(post);
            StatusLine statusLine = response.getStatusLine();
            HttpEntity entity = response.getEntity();
            if (statusLine.getStatusCode() >= 300) {
                EntityUtils.consume(entity);
                throw new TransmissionException(statusLine.getReasonPhrase(), statusLine.getStatusCode());
            }
            return entity == null ? null : EntityUtils.toString(entity);
        } catch (SoapException ex) {
            throw ex;
        } catch (ConnectTimeoutException ex) {
            throw new TransmissionException("Connection timed out", ex);
        } catch (IOException ex) {
            throw new TransmissionException("Transmission failed", ex);
        } catch (RuntimeException ex) {
            post.abort();
            throw new TransmissionException("Transmission aborted", ex);
        }
    }

    // ----------------------------------------------------------------
    // INITIALIZATION API
    // ----------------------------------------------------------------
    private void initialize() {
        configureClient();
        configureAuthentication();
        configureTls();
        configureProxy();
    }

    private void configureClient() {
        client = new DefaultHttpClient();
        HttpParams httpParameters = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(httpParameters, connectTimeoutInMillis);
        HttpConnectionParams.setSoTimeout(httpParameters, readTimeoutInMillis);
        client.setParams(httpParameters);
    }

    private void configureAuthentication() {
        configureAuthentication(endpointUri, endpointProperties);
        configureAuthentication(proxyUri, proxyProperties);
    }

    private void configureAuthentication(URI uri, Security security) {
        if (security.isAuthEnabled()) {
            AuthScope scope = new AuthScope(uri.getHost(), uri.getPort());
            Credentials credentials = null;
            if (security.isAuthBasic()) {
                credentials = new UsernamePasswordCredentials(security.getAuthUsername(), security.getAuthPassword());
            } else if (security.isAuthDigest()) {
                credentials = new UsernamePasswordCredentials(security.getAuthUsername(), security.getAuthPassword());
            } else if (security.isAuthNtlm()) {
                // TODO
                credentials = new NTCredentials(security.getAuthUsername(), security.getAuthPassword(), null, null);
            } else if (security.isAuthSpnego()) {
                // TODO
            }
            client.getCredentialsProvider().setCredentials(scope, credentials);
        }
    }

    private void configureTls() {
        SSLSocketFactory factory;
        int port;
        try {
            if (endpointTlsEnabled && proxyTlsEnabled) {
                factory = SSLUtils.getMergedSocketFactory(endpointProperties, proxyProperties);
                registerTlsScheme(factory, proxyUri.getPort());
            } else if (endpointTlsEnabled) {
                factory = SSLUtils.getFactory(endpointProperties);
                port = endpointUri.getPort();
                registerTlsScheme(factory, port);
            } else if (proxyTlsEnabled) {
                factory = SSLUtils.getFactory(proxyProperties);
                port = proxyUri.getPort();
                registerTlsScheme(factory, port);
            }
        } catch (GeneralSecurityException ex) {
            throw new SoapClientException(ex);
        }
    }

    private void registerTlsScheme(SchemeLayeredSocketFactory factory, int port) {
        Scheme sch = new Scheme(HTTPS, port, factory);
        client.getConnectionManager().getSchemeRegistry().register(sch);
    }

    private void configureProxy() {
        if (proxyUri == null) {
            return;
        }
        if (proxyTlsEnabled) {
            final HttpHost proxy = new HttpHost(proxyUri.getHost(), proxyUri.getPort(), HTTPS);
            // https://issues.apache.org/jira/browse/HTTPCLIENT-1318
            // http://stackoverflow.com/questions/15048102/httprouteplanner-how-does-it-work-with-an-https-proxy
            // To make the HttpClient talk to a HTTP End-site through an HTTPS Proxy, the route should be secure,
            //  but there should not be any Tunnelling or Layering.
            if (!endpointTlsEnabled) {
                client.setRoutePlanner(new HttpRoutePlanner() {
                    @Override
                    public HttpRoute determineRoute(HttpHost target, HttpRequest request, HttpContext context) {
                        return new HttpRoute(target, null, proxy, true, RouteInfo.TunnelType.PLAIN, RouteInfo.LayerType.PLAIN);
                    }
                });
            }
            client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
        } else {
            HttpHost proxy = new HttpHost(proxyUri.getHost(), proxyUri.getPort());
            client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
        }
    }


    // ----------------------------------------------------------------
    // BUILDER API
    // ----------------------------------------------------------------
    private SoapClient() {
    }

    /**
     * Builder to construct a properly populated SoapClient
     */
    public static class Builder {

        private Integer readTimeoutInMillis = INFINITE_TIMEOUT;
        private Integer connectTimeoutInMillis = INFINITE_TIMEOUT;

        private URI endpointUri;
        private Security endpointProperties;
        private boolean endpointTlsEnabled;

        private URI proxyUri;
        private Security proxyProperties;
        private boolean proxyTlsEnabled;

        /**
         * @param value URL of the SOAP endpoint to whom the client should send messages. Null is not accepted.
         * @return builder
         */
        public Builder endpointUri(String value) {
            checkNotNull(value);
            try {
                URI uri = new URI(value);
                return endpointUri(uri);
            } catch (URISyntaxException ex) {
                throw new SoapClientException(String.format("URI [%s] is malformed", value) + ex.getMessage(), ex);
            }
        }

        /**
         * @param value URL of the SOAP endpoint to whom the client should send messages. Null is not accepted.
         * @return builder
         */
        public Builder endpointUri(URI value) {
            endpointUri = checkNotNull(value);
            endpointTlsEnabled = value.getScheme().equalsIgnoreCase(HTTPS);
            return this;
        }

        /**
         * @param value URL of the SOAP endpoint to whom the client should send messages. Null is not accepted.
         * @return builder
         */
        public Builder proxyUri(String value) {
            checkNotNull(value);
            try {
                URI uri = new URI(value);
                return proxyUri(uri);
            } catch (URISyntaxException ex) {
                throw new SoapClientException(String.format("URI [%s] is malformed", value), ex);
            }
        }

        /**
         * @param value URL of the SOAP endpoint to whom the client should send messages. Null is not accepted.
         * @return builder
         */
        public Builder proxyUri(URI value) {
            proxyUri = checkNotNull(value);
            proxyTlsEnabled = value.getScheme().equalsIgnoreCase(HTTPS);
            return this;
        }

        public Builder endpointSecurity(Security value) {
            this.endpointProperties = checkNotNull(value);
            return this;
        }

        public Builder proxySecurity(Security value) {
            this.proxyProperties = checkNotNull(value);
            return this;
        }

        /**
         * @param value Specifies the timeout in millisecond for the read operation. Has to be not negative.
         * @return builder
         */
        public Builder readTimeoutInMillis(int value) {
            checkArgument(value >= 0);
            readTimeoutInMillis = value;
            return this;
        }

        /**
         * @param value Specifies the timeout in millisecond for the connect operation. Has to be not negative.
         * @return builder
         */
        public Builder connectTimeoutInMillis(int value) {
            checkArgument(value >= 0);
            connectTimeoutInMillis = value;
            return this;
        }

        /**
         * Constructs properly populated soap client
         *
         * @return properly populated soap clients
         */
        public SoapClient build() {
            return initializeClient();
        }

        private SoapClient initializeClient() {
            SoapClient client = new SoapClient();
            client.endpointUri = endpointUri;
            if (endpointProperties == null) {
                endpointProperties = Security.builder().build();
            }
            client.endpointProperties = endpointProperties;
            client.endpointTlsEnabled = endpointTlsEnabled;

            client.proxyUri = proxyUri;
            if (proxyProperties == null) {
                proxyProperties = Security.builder().build();
            }
            client.proxyProperties = proxyProperties;
            client.proxyTlsEnabled = proxyTlsEnabled;

            client.readTimeoutInMillis = readTimeoutInMillis;
            client.connectTimeoutInMillis = connectTimeoutInMillis;

            client.initialize();
            return client;
        }
    }

    /**
     * @return a new instance of a SoapClient Builder
     */
    public static Builder builder() {
        return new Builder();
    }

}