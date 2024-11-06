package net.plumbing.msgbus.threads.utils;

import com.google.common.escape.Escaper;
import com.jayway.jsonpath.*;

import net.plumbing.msgbus.common.json.JSONException;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.model.*;
import net.plumbing.msgbus.threads.TheadDataAccess;
import org.apache.commons.io.IOUtils;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;
import org.slf4j.Logger;
import net.plumbing.msgbus.common.XMLchars;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
//import java.security.KeyManagementException;
//import java.security.KeyStoreException;
//import java.security.NoSuchAlgorithmException;

//import javax.net.ssl.SSLContext;
//import javax.security.cert.CertificateException;
//import javax.security.cert.X509Certificate;
//import java.security.cert.CertificateException;
//import java.security.cert.X509Certificate;

import javax.validation.constraints.NotNull;
//import javax.xml.parsers.DocumentBuilderFactory;
//import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
//import java.sql.RowId;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
//import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import java.util.concurrent.TimeUnit;
// import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;

import static net.plumbing.msgbus.common.XMLchars.OpenTag;
import static net.plumbing.msgbus.threads.utils.MessageUtils.stripNonValidXMLCharacters;

public class MessageHttpSend {

    /*public static SSLContext getSSLContext() {
        SSLContext sslContext;
        try {
             sslContext = new SSLContextBuilder()
                     .loadTrustMaterial(null, new TrustSelfSignedStrategy() {
                public boolean isTrusted (X509Certificate[] chain, String authType) throws CertificateException
                {
                    return true;
                }
            }).build();
        } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
            e.printStackTrace();
            sslContext=null;
        }
        return sslContext;
    }
    */

    public static int sendSoapMessage(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess, Logger MessageSend_Log) {
        // рассчитываем размер SoapEnvelope
        int SoapEnvelopeSize ;
        boolean IsDebugged = messageDetails.MessageTemplate4Perform.getIsDebugged();
        if ( messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty) >= 0 )
            SoapEnvelopeSize= XMLchars.Envelope_Begin.length() + messageDetails.XML_MsgSEND.length() + XMLchars.Envelope_End.length() +16 ;
        else
            SoapEnvelopeSize=(XMLchars.Envelope_Begin.length() + XMLchars.Header_Begin.length() + messageDetails.Soap_HeaderRequest.length() + XMLchars.Header_End.length() +
                              XMLchars.Body_Begin.length() + messageDetails.XML_MsgSEND.length() + XMLchars.Body_End.length() + XMLchars.Envelope_End.length() +16);

        // создаём с запасом в 16 символов
        StringBuilder SoapEnvelope = new StringBuilder( SoapEnvelopeSize );
        SoapEnvelope.append(XMLchars.Envelope_Begin);
        //MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] Sending SoapEnvelope-1[" + SoapEnvelope + "]" );
        if ( messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty) >= 0 )
            // Header_is_empty !
            ;
            //SoapEnvelope.append(SoapEnvelope);
        else {
            SoapEnvelope.append(XMLchars.Header_Begin);
            SoapEnvelope.append(messageDetails.Soap_HeaderRequest);
            SoapEnvelope.append(XMLchars.Header_End);
        }
        //MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] Sending SoapEnvelope-2[" + SoapEnvelope + "]" );
        SoapEnvelope.append(XMLchars.Body_Begin);
        SoapEnvelope.append(messageDetails.XML_MsgSEND);
        //MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] Sending messageDetails.XML_MsgSEND[" + messageDetails.XML_MsgSEND + "]" );
        SoapEnvelope.append(XMLchars.Body_End);
        SoapEnvelope.append(XMLchars.Envelope_End);
        //MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "] Sending SoapEnvelope-3[" + SoapEnvelope + "]" );

        MessageTemplate4Perform messageTemplate4Perform = messageDetails.MessageTemplate4Perform;

        String EndPointUrl;
        if ( StringUtils.substring(messageTemplate4Perform.getEndPointUrl(),0,"http".length()).equalsIgnoreCase("http") )
            EndPointUrl = messageTemplate4Perform.getEndPointUrl();
        else
            EndPointUrl = "http://" + messageTemplate4Perform.getEndPointUrl();

        HttpClient ApiRestHttpClient;
        String PropUser = messageDetails.MessageTemplate4Perform.getPropUser();
        String PropPswd = messageDetails.MessageTemplate4Perform.getPropPswd();
        if ( (messageDetails.MessageTemplate4Perform.restPasswordAuthenticator != null) &&
                (!messageDetails.MessageTemplate4Perform.getIsPreemptive())  // adding the header to the HttpRequest and removing Authenticator
        )
        {
            if ( IsDebugged ) {
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendSoapMessage.POST PropUser=`" + PropUser + "` PropPswd=`" + PropPswd + "`");
            }
            ApiRestHttpClient = HttpClient.newBuilder()
                    .authenticator( messageDetails.MessageTemplate4Perform.restPasswordAuthenticator )
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds( messageTemplate4Perform.getPropTimeout_Conn()))
                    .build();
        }
        else {
            if ( IsDebugged )
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendSoapMessage.POST PropUser== null (`" + PropUser + "`)" );
            ApiRestHttpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .connectTimeout(Duration.ofSeconds(messageTemplate4Perform.getPropTimeout_Conn()))
                    .build();
        }

        // TODO : for Ora RowId ROWID_QUEUElog=null;
        String ROWID_QUEUElog=null;
        String RestResponse=null;

        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());

        byte[] RequestBody;
        if (( messageDetails.MessageTemplate4Perform.getPropEncoding_Out() !=null ) &&
                ( !messageDetails.MessageTemplate4Perform.getPropEncoding_Out().equals("UTF-8" )) ) {
            try {
                RequestBody = SoapEnvelope.toString().getBytes( messageDetails.MessageTemplate4Perform.getPropEncoding_Out());
            } catch (UnsupportedEncodingException encodingExc) {
                System.err.println("[" + messageQueueVO.getQueue_Id() + "] sendSoapMessage.POST UnsupportedEncodingException");
                encodingExc.printStackTrace();
                MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] from " + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() + " to_UTF_8 fault:" + encodingExc);
                messageDetails.MsgReason.append(" sendSoapMessage.POST" + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() +  " fault: " + sStackTrace.strInterruptedException(encodingExc));
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendSoapMessage.POST", true, encodingExc, MessageSend_Log);
                // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null, monitoringQueueVO, MessageSend_Log);
                return -1;
            }
        }
        else
            RequestBody = SoapEnvelope.toString().getBytes ( StandardCharsets.UTF_8 );
    try
    {

        try { // Готовим HTTP-запрос
            if ( IsDebugged )
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.POST(" + EndPointUrl + ").connectTimeoutInMillis=" + messageTemplate4Perform.getPropTimeout_Conn() +
                    ".000;.readTimeoutInMillis=" + messageTemplate4Perform.getPropTimeout_Read() +
                    ".000;.PropUser=" + messageDetails.MessageTemplate4Perform.getPropUser() +
                    ";.PropPswd=" + messageDetails.MessageTemplate4Perform.getPropPswd() +
                    ";."+ messageDetails.MessageTemplate4Perform.SOAP_ACTION_11 + "=" + messageDetails.MessageTemplate4Perform.getSOAPAction()
            );
            if ( IsDebugged )
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.POST[" + SoapEnvelope + "]" );
            messageDetails.Confirmation.clear();
            messageDetails.XML_MsgResponse.setLength(0); messageDetails.XML_MsgResponse.trimToSize();

            String SOAPAction=messageDetails.MessageTemplate4Perform.getSOAPAction();
            if ( SOAPAction == null)
                SOAPAction= "";
            // InputStream parsedMessageStream = new ByteArrayInputStream(SoapEnvelope.toString().getBytes(StandardCharsets.UTF_8));
            if ( IsDebugged )
                ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog( messageQueueVO.getQueue_Id(), SoapEnvelope.toString(), MessageSend_Log );


            HttpRequest.Builder requestBuilder = java.net.http.HttpRequest.newBuilder();
            if ( (messageDetails.MessageTemplate4Perform.restPasswordAuthenticator != null) &&
                    (messageDetails.MessageTemplate4Perform.getIsPreemptive())  // adding the header to the HttpRequest
            ) {  // добавляем Authorization заголовки через HttpRequest.Builder
                String encodedAuth = Base64.getEncoder()
                        .encodeToString((PropUser + ":" + PropPswd ).getBytes(StandardCharsets.UTF_8));
                requestBuilder = requestBuilder
                                .header("Authorization", "Basic " + encodedAuth );
            }

            java.net.http.HttpRequest request = requestBuilder
                            .POST( HttpRequest.BodyPublishers.ofByteArray(RequestBody) )
                            .uri(URI.create(EndPointUrl))
                            .setHeader("Content-Type", "text/xml;charset=UTF-8")
                            .setHeader("User-Agent", "msgBus/Java-21") // add request header
                            .setHeader(messageDetails.MessageTemplate4Perform.SOAP_ACTION_11,SOAPAction)
                            .setHeader("Accept", "*/*")
                            .timeout( Duration.ofSeconds( messageTemplate4Perform.getPropTimeout_Read()) )
                            .build();

            HttpResponse<byte[]> Response = ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofByteArray() );
            int restResponseStatus = Response.statusCode();

            // Headers headers = Response.getHeaders();
            // MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Response getHeaders()=" + headers.all().toString() +" getHeaders().size=" + headers.size() );

            MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "]" +"sendSoapMessage.POST.Response.getBody().length =" + Response.body().length );
            //  byte [] RequestBodyContent = new Response.toString();
            // перекодируем ответ из кодировки, которая была указана в шаблоне для внешней системы в UTF_8
            // Response => RestResponse;
            try {
                if (( messageDetails.MessageTemplate4Perform.getPropEncoding_Out() !=null ) &&
                    ( !messageDetails.MessageTemplate4Perform.getPropEncoding_Out().equals("UTF-8" )) ) {
                    // RestResponse = IOUtils.toString(Response, messageDetails.MessageTemplate4Perform.getPropEncoding_Out() ); //StandardCharsets.UTF_8);
                    RestResponse = IOUtils.toString( Response.body(), messageDetails.MessageTemplate4Perform.getPropEncoding_Out() ); //StandardCharsets.UTF_8);
                }
                else RestResponse = IOUtils.toString( Response.body(), "UTF-8" ); //StandardCharsets.UTF_8);
            }
            catch (Exception ioExc) {
                System.err.println( "["+ messageQueueVO.getQueue_Id()  + "] IOUtils.toString.UnsupportedEncodingException" );
                ioExc.printStackTrace();
                MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] IOUtils.toString from " +
                        ( messageDetails.MessageTemplate4Perform.getPropEncoding_Out() ==  null ? "UTF_8" : messageDetails.MessageTemplate4Perform.getPropEncoding_Out() )
                        + " to_UTF_8 fault:" + ioExc );
                messageDetails.MsgReason.append(" sendSoapMessage.POST.to_UTF_8 fault: ").append ( sStackTrace.strInterruptedException(ioExc));
                MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                        "sendSoapMessage.POST", true,  ioExc ,  MessageSend_Log);
                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                return -1;
            }

//            сохраняем в XML_MsgResponse SOAP-конверт уже в UTF_8
            messageDetails.XML_MsgResponse.append( RestResponse ); // XML_MsgResponse был очищен

            // -- Задваивается в случае ошибки => это делается внутри ProcessingSendError()
            // messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);
            if ( IsDebugged )
            MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] HTTP status: " + restResponseStatus + " sendSoapMessage.POST.Response=#(`{}`)", messageDetails.XML_MsgResponse.toString());
            if ( IsDebugged )
                theadDataAccess.doUPDATE_QUEUElog( ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse, MessageSend_Log );

        } catch ( Exception e) {
            // Журналируем ответ как есть
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.POST ("+EndPointUrl+") fault:" + e );
            messageDetails.MsgReason.append(" sendSoapMessage.POST (" ).append ( EndPointUrl )
                                    .append ( ") fault: " ).append ( sStackTrace.strInterruptedException(e));

            if ( IsDebugged )
                theadDataAccess.doUPDATE_QUEUElog( ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e), MessageSend_Log );

            // HE-4892 Если транспорт отвалился , то Шина ВСЁ РАВНО формирует как бы ответ , но с Fault внутри.
            // НАДО проверять количество порыток !!!
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Retry_Count ("+messageQueueVO.getRetry_Count()+")>= " +
                    "( ShortRetryCount=" +messageDetails.MessageTemplate4Perform.getShortRetryCount() +
                    " LongRetryCount=" + messageDetails.MessageTemplate4Perform.getLongRetryCount() + ")" );
            if ( messageQueueVO.getRetry_Count() +1  >= messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount() )
            {
                // количество порыток исчерпано, формируем результат для выхода из повторов
                MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.POST (" + EndPointUrl + ") fault:" + e);
                messageDetails.XML_MsgResponse.setLength(0);
                messageDetails.XML_MsgResponse.append(XMLchars.Envelope_Begin);
                messageDetails.XML_MsgResponse.append(XMLchars.Body_Begin);
                messageDetails.XML_MsgResponse.append(XMLchars.Fault_Begin);
                messageDetails.XML_MsgResponse.append("sendSoapMessage(`" ).append( EndPointUrl ).append( "`) fault:" ).append( e.getMessage());
                messageDetails.XML_MsgResponse.append(XMLchars.Fault_End);
                messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
                messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);

                MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                        "sendSoapMessage.POST (" + EndPointUrl + "), do re-Send: ", false,  e ,  MessageSend_Log);
            }
            else {
                // HE-4892 Если транспорт отвалился , то Шина выставляет RESOUT - коммент ProcessingSendError & return -1;
                 MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                        "sendSoapMessage.POST (" + EndPointUrl + ") ", true,  e ,  MessageSend_Log);
                 return -1;
            }
        }
        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );

        try {
            // Получили ответ от сервиса, инициируем обработку SOAP getResponseBody()
            MessageSoapSend.getResponseBody (messageDetails, null, MessageSend_Log);
            if ( IsDebugged )
            MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage:ClearBodyResponse=(" + messageDetails.XML_ClearBodyResponse.toString() + ")");
            else {// HE-9187
                if (messageDetails.XML_ClearBodyResponse.length() > 2049)
                    MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage:ClearBodyResponse[2048 char]=(" +
                            messageDetails.XML_ClearBodyResponse.substring(0, 2048)
                            + "...)");
                else
                    MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage:ClearBodyResponse[all char]=(" +
                            messageDetails.XML_ClearBodyResponse.toString() + "...)");
                // client.wait(2048); --HE-10763 : Расширить размер логируемого сообщения ( ответ при ответ при сбое на стороне получателя ) до 2 кб
            }

        } catch (Exception e) {
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] Retry_Count=" + messageQueueVO.getRetry_Count() + " SendSoapMessage.getResponseBody fault(" + RestResponse + " : " + sStackTrace.strInterruptedException(e));
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] Sending SoapEnvelope[" + SoapEnvelope + "]" );
            messageDetails.MsgReason.append(" sendSoapMessage.getResponseBody fault: " ).append ( sStackTrace.strInterruptedException(e));

            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "sendSoapMessage.getResponseBody" , true,  e ,  MessageSend_Log);
            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, SoapEnvelope.toString(),
            //        "sendSoapMessage.Unirest.post (" + EndPointUrl + ") " + sStackTrace.strInterruptedException(e),monitoringQueueVO, MessageSend_Log);
            return -3;
        }
        // когда всё хорошо, увеличивать счётчик нет смысла
        // messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);
    } // делаем, всё,  что можно и нужно
    catch ( Exception allE) {
        if (ApiRestHttpClient != null)
        try {
            ApiRestHttpClient.shutdown();
            ApiRestHttpClient.close();

        } catch ( Exception IOE ) {
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.ApiRestHttpClient.close fault, Exception:" + IOE.getMessage());
        }
        ApiRestHttpClient = null;
        /*try {
            syncConnectionManager.close();
        } catch ( Exception anyE ) {
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "HttpGetMessage.Unirest.syncConnectionManager.close fault, UnirestException:" + anyE);
        }
        syncConnectionManager = null;*/

    } finally {
        MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.ApiRestHttpClient.close finally" );
        if (ApiRestHttpClient != null)
            try {
                ApiRestHttpClient.close();

            } catch ( Exception IOE ) {
                MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.ApiRestHttpClient.close finally fault, Exception:" + IOE.getMessage());
            }
        ApiRestHttpClient = null;
        /*
        if (syncConnectionManager != null)
            try {
                syncConnectionManager.shutdown();
                syncConnectionManager.close();
            } catch ( Exception anyE ) {
                MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() +"]" + "HttpGetMessage.Unirest.syncConnectionManager.close finally fault, UnirestException:" + anyE);
            }
        syncConnectionManager = null;
        */
    }
        return 0;
    } // sendSoapMessage
    /*
    private static CloseableHttpClient getCloseableHttpClient( MessageQueueVO messageQueueVO, MessageDetails Message , TheadDataAccess theadDataAccess,
                                                               BasicHttpClientConnectionManager syncConnectionManager, int Timeout_Conn, int Timeout_Read,
                                                       Logger MessegeReceive_Log) {
        int ReadTimeoutInMillis = Timeout_Read * 1000;
        int ConnectTimeoutInMillis = Timeout_Conn * 1000;
        SSLContext sslContext = MessageHttpSend.getSSLContext(  );
        if ( sslContext == null ) {
            MessegeReceive_Log.error("["+ messageQueueVO.getQueue_Id()+"] " + "SSLContextBuilder fault: (" +  Message.MsgReason.toString() + ")");
            Message.MsgReason.append("Внутренняя Ошибка SSLContextBuilder fault: (" +  Message.MsgReason.toString() + ")" ) ;

            MessageUtils.ProcessingSendError(messageQueueVO, Message, theadDataAccess,
                    "Внутренняя Ошибка SSLContextBuilder fault: (" +  Message.MsgReason.toString() + ")",
                    true, null , MessegeReceive_Log );
            return null;
        }

        /// это в вызывающем методе !- syncConnectionManager = new PoolingHttpClientConnectionManager();
        // для BasicHttpClientConnectionManager не нужно
        //syncConnectionManager.setMaxTotal((Integer) 4);
        // syncConnectionManager.setDefaultMaxPerRoute((Integer) 2);
        RequestConfig rc;

        rc = RequestConfig.custom()
                .setConnectionRequestTimeout(ConnectTimeoutInMillis)
                .setConnectTimeout(ConnectTimeoutInMillis)
                .setSocketTimeout( ReadTimeoutInMillis)
                .build();

        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                .disableDefaultUserAgent()
                .disableRedirectHandling()
                .disableAutomaticRetries()
                .setUserAgent("msgbus.Sender")
                .setSSLContext(sslContext)
                .disableAuthCaching()
                .disableConnectionState()
                .disableCookieManagement()
                // .useSystemProperties() // HE-5663  https://stackoverflow.com/questions/5165126/without-changing-code-how-to-force-httpclient-to-use-proxy-by-environment-varia
                .setConnectionManager(syncConnectionManager)
                .setSSLHostnameVerifier(new NoopHostnameVerifier())
                .setConnectionTimeToLive( Timeout_Read +  Timeout_Conn + 5, TimeUnit.SECONDS)
                .evictIdleConnections((long) (Timeout_Read +  Timeout_Conn + 5)*2, TimeUnit.SECONDS);
        httpClientBuilder.setDefaultRequestConfig(rc);

        CloseableHttpClient
                ApiRestHttpClient = httpClientBuilder.build();
        if ( ApiRestHttpClient == null) {
            try {
                syncConnectionManager.shutdown();
                syncConnectionManager.close();
            } catch ( Exception e) {
                MessegeReceive_Log.error("["+ messageQueueVO.getQueue_Id()  +"] " + "Внутренняя ошибка - httpClientBuilder.build() не создал клиента. И ещё проблема с syncConnectionManager.shutdown()...");
                System.err.println("["+ messageQueueVO.getQueue_Id()  +"] " + "Внутренняя ошибка - httpClientBuilder.build() не создал клиента. И ещё проблема с syncConnectionManager.shutdown()..." + e.getMessage()); //e.printStackTrace();
            }
            MessegeReceive_Log.error("["+ messageQueueVO.getQueue_Id()  +"] " + "httpClientBuilder.build() fault");
            Message.MsgReason.append("Внутренняя Ошибка httpClientBuilder.build() fault");


            MessageUtils.ProcessingSendError(messageQueueVO, Message, theadDataAccess,
                    "Внутренняя Ошибка httpClientBuilder.build() fault: (" +  Message.MsgReason.toString() + ")",
                    true, null , MessegeReceive_Log );
            return null;
        }
        return ApiRestHttpClient  ;

    }
    */

    public static int sendPostMessage(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess, Logger MessageSend_Log) {
        //
        MessageTemplate4Perform messageTemplate4Perform = messageDetails.MessageTemplate4Perform;

        String EndPointUrl;
        String ROWID_QUEUElog=null;
        if ( StringUtils.substring(messageTemplate4Perform.getEndPointUrl(),0,"http".length()).equalsIgnoreCase("http") )
            EndPointUrl = messageTemplate4Perform.getEndPointUrl();
        else
            EndPointUrl = "http://" + messageTemplate4Perform.getEndPointUrl();

        // int ConnectTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Conn() * 1000;
        // int ReadTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Read() * 1000;
        String RestResponse;
        int restResponseStatus;
        String AckXSLT_4_make_JSON = messageTemplate4Perform.getAckXSLT() ;
        boolean IsDebugged = messageDetails.MessageTemplate4Perform.getIsDebugged();

        HttpClient ApiRestHttpClient;

        String PropUser = messageDetails.MessageTemplate4Perform.getPropUser();
        String PropPswd = messageDetails.MessageTemplate4Perform.getPropPswd();
        if ( (messageDetails.MessageTemplate4Perform.restPasswordAuthenticator != null) &&
             (!messageDetails.MessageTemplate4Perform.getIsPreemptive())  // adding the header to the HttpRequest and removing Authenticator
           )
        {
            if ( IsDebugged ) {
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.POST PropUser=`" + PropUser + "` PropPswd=`" + PropPswd + "`");
            }
            ApiRestHttpClient = HttpClient.newBuilder()
                    .authenticator( messageDetails.MessageTemplate4Perform.restPasswordAuthenticator )
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds( messageTemplate4Perform.getPropTimeout_Conn()))
                    .build();
        }
        else {
            if ( IsDebugged )
            MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.POST PropUser== null (`" + PropUser + "`)" );
            ApiRestHttpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .connectTimeout(Duration.ofSeconds(messageTemplate4Perform.getPropTimeout_Conn()))
                    .build();
        }
        byte[] RequestBody;

        Map<String, String> httpHeaders= new HashMap<>();
        String headerParams[];
        httpHeaders.put("User-Agent", "msgBus/Java-21");
        httpHeaders.put("Accept", "*/*");
        httpHeaders.put("Connection", "close");
        if ( (messageDetails.MessageTemplate4Perform.restPasswordAuthenticator != null) &&
                (messageDetails.MessageTemplate4Perform.getIsPreemptive())  // adding the header to the HttpRequest
        ) {
            String encodedAuth = Base64.getEncoder()
                                .encodeToString((PropUser + ":" + PropPswd ).getBytes(StandardCharsets.UTF_8));
            httpHeaders.put("Authorization", "Basic " + encodedAuth );
        }

        if ( AckXSLT_4_make_JSON != null )
        { httpHeaders.put("Content-Type","application/json;charset=UTF-8");
            if ( IsDebugged )
            MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.POST JSON `" + messageDetails.XML_MsgSEND + "`" );
        }
        else
        { httpHeaders.put("Content-Type", "text/xml;charset=UTF-8"); }

        if (( messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty) == -1 )// NOT Header_is_empty
           && ( ! messageDetails.Soap_HeaderRequest.isEmpty() ))
        {
            headerParams = messageDetails.Soap_HeaderRequest.toString().split(":");
            if ( IsDebugged ) {
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.POST headerParams.length=" + headerParams.length);
                for (int i = 0; i < headerParams.length; i++)
                    MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.POST headerParams[" + i + "] = " + headerParams[i]);
            }
            if (headerParams.length > 1  )
            for (int i = 0; i < headerParams.length; i++)
                httpHeaders.put(headerParams[0], headerParams[1]);
        }
        else { if ( IsDebugged )
            MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.POST indexOf(XMLchars.TagMsgHeaderEmpty)=" + messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty));
        }
        // MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.Unirest.post `" + messageDetails.Soap_HeaderRequest + "` httpHeaders.size=" + httpHeaders.size() );
        //+                 "; headerParams= " + headerParams.toString() );
    try {

        if ((messageDetails.MessageTemplate4Perform.getPropEncoding_Out() != null) &&
                (!messageDetails.MessageTemplate4Perform.getPropEncoding_Out().equalsIgnoreCase("UTF-8"))) {
            try {
                RequestBody = messageDetails.XML_MsgSEND.getBytes(messageDetails.MessageTemplate4Perform.getPropEncoding_Out());
            } catch (UnsupportedEncodingException e) {
                System.err.println("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage: UnsupportedEncodingException");
                e.printStackTrace();
                MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] from " + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() + " to_UTF_8 fault:" + e);
                messageDetails.MsgReason.append(" sendPostMessage.post.to").append(messageDetails.MessageTemplate4Perform.getPropEncoding_Out()).append(" fault: ").append(sStackTrace.strInterruptedException(e));
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendPostMessage.POST", true, e, MessageSend_Log);
                return -1;
            }
        } else
            RequestBody = messageDetails.XML_MsgSEND.getBytes(StandardCharsets.UTF_8);

        try {
            // Unirest.config().httpClient(ApiRestHttpClient); //( messageDetails.SimpleHttpClient);
            if ( IsDebugged )
            MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.POST(" + EndPointUrl + ").connectTimeoutInMillis=" + messageTemplate4Perform.getPropTimeout_Conn() +
                    ";.readTimeoutInMillis=ReadTimeoutInMillis= " + messageTemplate4Perform.getPropTimeout_Read() + " PropUser:" + PropUser);
            messageDetails.Confirmation.clear();
            messageDetails.XML_MsgResponse.setLength(0);

            if (IsDebugged)
                ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog(messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND.toString(), MessageSend_Log);

            HttpRequest.Builder requestBuilder = java.net.http.HttpRequest.newBuilder();
            // добавляем все заголовки как есть через HttpRequest.Builder
            for (Map.Entry<String, String> entry: httpHeaders.entrySet()) {
                requestBuilder = requestBuilder
                                .header(entry.getKey(),entry.getValue());
                if ( IsDebugged )
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.POST .header: `" + entry.getKey() + ":" + entry.getValue() + "`");
                // queryString.append(entry.getKey()).append("=").append(entry.getValue());
            }
            java.net.http.HttpRequest request = requestBuilder
                    .POST( HttpRequest.BodyPublishers.ofByteArray(RequestBody) )
                    .uri(URI.create(EndPointUrl))
                    .timeout( Duration.ofSeconds( messageTemplate4Perform.getPropTimeout_Read()) )
                    .build();

            HttpResponse<byte[]> Response = ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofByteArray() );
            restResponseStatus = Response.statusCode();

            //Test = Response.getBody();
            //Headers headers = Response.getHeaders();
            //MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Response getHeaders()=" + headers.all().toString() +" getHeaders().size=" + headers.size() );

            MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.Response httpCode=" + restResponseStatus + " getBody().length=" + Response.body().length);
            // MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Response getBody()=" + Arrays.toString(Test) +" getBody().length=" + Test.length );

            // перекодируем ответ из кодировки, которая была указана в шаблоне для внешней системы в UTF_8 RestResponse = Response.getBody();
            try {
                if ((messageDetails.MessageTemplate4Perform.getPropEncoding_Out() != null) &&
                        (!messageDetails.MessageTemplate4Perform.getPropEncoding_Out().equalsIgnoreCase("UTF-8"))) {
                    RestResponse = IOUtils.toString(Response.body(), messageDetails.MessageTemplate4Perform.getPropEncoding_Out()); //StandardCharsets.UTF_8);
                } else
                    RestResponse = stripNonValidXMLCharacters(IOUtils.toString(Response.body(), "UTF-8")); // StandardCharsets.UTF_8);

                if (IsDebugged)
                    theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse, MessageSend_Log);

            } catch (Exception ioExc) {
                String PropEncoding_Out;
                if (messageDetails.MessageTemplate4Perform.getPropEncoding_Out() == null) PropEncoding_Out = "UTF_8";
                else PropEncoding_Out = messageDetails.MessageTemplate4Perform.getPropEncoding_Out();
                System.err.println("[" + messageQueueVO.getQueue_Id() + "] IOUtils.toString.UnsupportedEncodingException: Encoding `" + PropEncoding_Out + "`");
                ioExc.printStackTrace();
                MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] IOUtils.toString from `" + PropEncoding_Out + "` to_UTF_8 fault:" + ioExc);
                messageDetails.MsgReason.append(" HttpGetMessage.post.to_UTF_8 Encoding fault `" + PropEncoding_Out + "` :").append(sStackTrace.strInterruptedException(ioExc));
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendPostMessage.POST", true, ioExc, MessageSend_Log);
                if (IsDebugged)
                    theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(ioExc), MessageSend_Log);
                return -1;
            }
            /**/

            //  формируем в XML_MsgResponse ответ а-ля SOAP
            messageDetails.XML_MsgResponse.append(XMLchars.Envelope_Begin);
            // --бессмысленно добавлять в Header, обработка берёт из /Body/MsgData , но для чтения лога буде полезно
                messageDetails.XML_MsgResponse.append(XMLchars.Header_Begin);
                    messageDetails.XML_MsgResponse.append( XMLchars.NameTagHttpStatusCode_Begin );
                        messageDetails.XML_MsgResponse.append(restResponseStatus);
                    messageDetails.XML_MsgResponse.append( XMLchars.NameTagHttpStatusCode_End );
                messageDetails.XML_MsgResponse.append(XMLchars.Header_End);

            messageDetails.XML_MsgResponse.append(XMLchars.Body_Begin);

            if (RestResponse.isEmpty()) {  // добавляем <HttpStatusCode>httpStatus</HttpStatusCode>
                append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , null );
                 } else // получили НЕпустой ответ, пробуем его разобрать
            {
                if (RestResponse.startsWith("<?xml") || RestResponse.startsWith("<?XML")) {
                    int index2 = RestResponse.indexOf("?>"); //6
                    messageDetails.XML_MsgResponse.append(RestResponse.substring(index2 + 2));
                    messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
                    messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);
                } else {
                    if (RestResponse.startsWith("<")) { // чтитаем, что в ответе XML
                        messageDetails.XML_MsgResponse.append(RestResponse);
                        messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
                        messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);

                    } else { // возможно, Json
                        if (RestResponse.startsWith("{")) { // Разбираем Json
                            try {
                                JSONObject RestResponseJSON = new JSONObject(RestResponse);
                                messageDetails.XML_MsgResponse.append(XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse));
                                messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
                                messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);
                            } catch (Exception JSONe) { // получили непонятно что
                                // Кладем полученный ответ в <MsgData><![CDATA[" RestResponse "]]></MsgData>
                                append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse );
                                }

                        } else {
                            // ответ и не `{` и не `<` - опять же получили непонятно что
                            // Кладем полученный ответ в <MsgData><![CDATA[" RestResponse "]]></MsgData>
                            append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse );
                        }
                    }
                }
            }

            if (IsDebugged)
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.POST Envelope_MsgResponse=(" + messageDetails.XML_MsgResponse.toString() + ")");


            // -- Задваивается в случае ошибки => это делается внутри ProcessingSendError()
            // messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);

        } catch (Exception e) {
            System.err.println("[" + messageQueueVO.getQueue_Id() + "]  Exception");
            e.printStackTrace();
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.POST (" + EndPointUrl + ") fault, UnirestException:" + e);
            messageDetails.MsgReason.append(" sendPostMessage.POST fault: ").append(sStackTrace.strInterruptedException(e));

            // Журналируем UnirestException-ответ как есть
            if (IsDebugged)
                theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e), MessageSend_Log);

            // HE-4892 Если транспорт отвалился , то Шина ВСЁ РАВНО формирует как бы ответ , но с Fault внутри.
            // НАДО проверять количество порыток !!!
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Retry_Count (" + messageQueueVO.getRetry_Count() + ")>= " +
                    "( ShortRetryCount=" + messageDetails.MessageTemplate4Perform.getShortRetryCount() +
                    " LongRetryCount=" + messageDetails.MessageTemplate4Perform.getLongRetryCount() + ")");
            if (messageQueueVO.getRetry_Count() + 1 >= messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount()) {
                // количество порыток исчерпано, формируем результат для выхода из повторов
                MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.POST (" + EndPointUrl + ") fault:" + e);
                append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, 506 ,
                        "sendPostMessage (").append(EndPointUrl).append(") fault:").append(e.getMessage() );

                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendPostMessage.POST (" + EndPointUrl + "), do re-Send: ", false, e, MessageSend_Log);
            } else {
                // HE-4892 Если транспорт отвалился , то Шина выставляет RESOUT - коммент ProcessingSendError & return -1;
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendPostMessage.POST (" + EndPointUrl + ") ", true, e, MessageSend_Log);
                return -1;
            }
            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    "sendPostMessage.POST(" + EndPointUrl + ")", true, e, MessageSend_Log);
            return -1;
        }

        if (IsDebugged)
            MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.POST httpStatus=[" + restResponseStatus + "], RestResponse=(" + RestResponse + ")");

        try {
            // Получили ответ от сервиса, инициируем обработку getResponseBody()
            InputStream parsedRestResponseStream;
            parsedRestResponseStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
            SAXBuilder documentBuilder = new SAXBuilder();
            Document XMLdocument;

            try {
                XMLdocument = documentBuilder.build(parsedRestResponseStream);
                if (IsDebugged)
                    MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage documentBuilder=[" + XMLdocument.toString() + "], XML_MsgResponse=(" + messageDetails.XML_MsgResponse + ")");

            } catch (JDOMException RestResponseE) {
                XMLdocument = null;
                MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.documentBuilder fault: " + sStackTrace.strInterruptedException(RestResponseE));
                // формируем искуственный XML_MsgResponse из Fault ,  меняем XML_MsgResponse
                append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse );
            }

            MessageSoapSend.getResponseBody(messageDetails, XMLdocument, MessageSend_Log);

            if (IsDebugged)
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage:ClearBodyResponse=(" + messageDetails.XML_ClearBodyResponse.toString() + ")");
            // client.wait(100);

        } catch (Exception e) {
            System.err.println("[" + messageQueueVO.getQueue_Id() + "]  Exception");
            e.printStackTrace();
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.getResponseBody fault: " + sStackTrace.strInterruptedException(e));
            messageDetails.MsgReason.append(" sendPostMessage.getResponseBody fault: ").append(sStackTrace.strInterruptedException(e));

            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    "sendPostMessage.getResponseBody", true, e, MessageSend_Log);
            return -3;
        }
        if (restResponseStatus != 200) // Rest вызов считаем успешным только при получении
        {
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.restResponseStatus != 200: " + restResponseStatus);
            messageDetails.MsgReason.append(" sendPostMessage.restResponseStatus != 200: ").append(restResponseStatus);

            int messageRetry_Count = MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    "sendPostMessage.restResponseStatus != 200 ", false, null, MessageSend_Log);
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.messageRetry_Count = " + messageRetry_Count);
            if ( messageDetails.XML_ClearBodyResponse.length() > XMLchars.nanXSLT_Result.length() )
                return 0; // ответ от внешней системы разобран в виде XML , надо продолжить обработку
            else
                return -5; // и restResponseStatus != 200 и ответ неразбрчив
        } else
            return 0;
     } catch ( Exception allE) {
        if (ApiRestHttpClient != null)
        try {
            ApiRestHttpClient.close();

        } catch ( Exception IOE ) {
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() +"]" + "sendPostMessage.ApiRestHttpClient.close fault, Exception:" + IOE.getMessage());
        }
        ApiRestHttpClient = null;
        /*
        try {
            syncConnectionManager.shutdown();
            syncConnectionManager.close();
        } catch ( Exception anyE ) {
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() +"]" + "sendPostMessage.Unirest.syncConnectionManager.close fault, UnirestException:" + anyE);
        }
        syncConnectionManager = null;*/

    } finally {
        MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() +"]" + "sendPostMessage.ApiRestHttpClient.close finally" );
        if (ApiRestHttpClient != null)
            try {
                ApiRestHttpClient.close();

            } catch ( Exception IOE ) {
                MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() +"]" + "sendPostMessage.ApiRestHttpClient.close finally fault, UnirestException:" + IOE.getMessage());
            }
        ApiRestHttpClient = null;
        /*if (syncConnectionManager != null)
            try {
                syncConnectionManager.shutdown();
                syncConnectionManager.close();
            } catch ( Exception anyE ) {
                MessageSend_Log.error("[" + "]" + "sendPostMessage.Unirest.syncConnectionManager.close finally fault, UnirestException:" + anyE);
            }
        syncConnectionManager = null;*/
    }
        return 0;
    }


    private static StringBuilder append_Http_ResponseStatus_and_PlaneResponse ( @NotNull StringBuilder XML_MsgResponse, @NotNull Integer restResponseStatus, String restResponse )
    {
        XML_MsgResponse.setLength(0);
        XML_MsgResponse.trimToSize();
        if ( (restResponseStatus < 200) || (restResponseStatus > 299)  ) {

            XML_MsgResponse.append(XMLchars.Fault_ExtResponse_Begin)
                            .append(restResponseStatus)
                            .append(XMLchars.FaultExtResponse_FaultString);
            if (restResponse!= null)
            XML_MsgResponse.append(restResponse);
            //else XML_MsgResponse.append("");
            XML_MsgResponse.append(XMLchars.FaultExtResponse_End);
        }
        else {

            XML_MsgResponse.append(XMLchars.Success_ExtResponse_Begin)
                        .append(restResponseStatus)
                        .append(XMLchars.Success_ExtResponse_PayloadString);
            if (restResponse!= null)
                XML_MsgResponse.append(restResponse);
            //else XML_MsgResponse.append("");
            XML_MsgResponse.append(XMLchars.Success_ExtResponse_End);
        }

        return XML_MsgResponse;
    }


///////////////////////////////////// HttpGetMessage //////////////////////////////////////////////////////////////////////////////////////////////////////

    public static int HttpGetMessage(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess, Logger MessageSend_Log) {

		MessageTemplate4Perform messageTemplate4Perform = messageDetails.MessageTemplate4Perform;
        boolean IsDebugged = messageDetails.MessageTemplate4Perform.getIsDebugged();
		String EndPointUrl;
            if ( StringUtils.substring(messageTemplate4Perform.getEndPointUrl(),0,"http".length()).equalsIgnoreCase("http") )
                EndPointUrl = messageTemplate4Perform.getEndPointUrl();
            else
                EndPointUrl = "http://" + messageTemplate4Perform.getEndPointUrl();

		//int ConnectTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Conn() * 1000;
		// int ReadTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Read() * 1000;
        String RestResponse=null;
        HttpResponse <String> RestResponseGet;
        Integer restResponseStatus;

        HashMap<String, String > HttpGetParams = new HashMap<String, String >();

        int numOfParams;
        try {
            numOfParams = setHttpGetParams( messageQueueVO.getQueue_Id() , messageDetails.XML_MsgSEND, HttpGetParams, messageDetails.MessageTemplate4Perform.getPropEncoding_Out(), MessageSend_Log );
        }
        catch ( Exception e) {
            if (e instanceof UnsupportedEncodingException ) {
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "HttpGetMessage.setHttpGetParams() [" + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() + " не поддерживается]", true, e, MessageSend_Log);
            }
            else
            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "HttpGetMessage.setHttpGetParams() [не содержит параметров для HtthGet]", true,  e ,  MessageSend_Log);
            return -1;
        }
        if ( numOfParams < 1) {
            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "HttpGetMessage.setHttpGetParams() [не содержит параметров для HtthGet]", true,  null ,  MessageSend_Log);
            return -1;
        }

        HttpClient ApiRestHttpClient;
        String PropUser = messageDetails.MessageTemplate4Perform.getPropUser();
        String PropPswd = messageDetails.MessageTemplate4Perform.getPropPswd();
        if ( (messageDetails.MessageTemplate4Perform.restPasswordAuthenticator != null) &&
                (!messageDetails.MessageTemplate4Perform.getIsPreemptive())  // adding the header to the HttpRequest and removing Authenticator
        )
        {
            if ( IsDebugged ) {
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] HttpGetMessage.GET PropUser=`" + PropUser + "` PropPswd=`" + PropPswd + "`");
            }
            ApiRestHttpClient = HttpClient.newBuilder()
                    .authenticator( messageDetails.MessageTemplate4Perform.restPasswordAuthenticator )
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds( messageTemplate4Perform.getPropTimeout_Conn()))
                    .build();
        }
        else {
            if ( IsDebugged )
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] HttpGetMessage.GET PropUser== null (`" + PropUser + "`)" );
            ApiRestHttpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .connectTimeout(Duration.ofSeconds(messageTemplate4Perform.getPropTimeout_Conn()))
                    .build();
        }

        //  TODO for Oracle ROWID, в случае Postgree String :
        // RowId ROWID_QUEUElog=null;
        String ROWID_QUEUElog=null;
  try {

      try {
          MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "HttpGetMessage.GET(" + EndPointUrl + ").connectTimeoutInMillis=" + messageTemplate4Perform.getPropTimeout_Conn() +
                  ";.readTimeoutInMillis=ReadTimeoutInMillis= " + messageTemplate4Perform.getPropTimeout_Read() +
                  "; User=" + messageDetails.MessageTemplate4Perform.getPropUser() +
                  "; Pswd" + messageDetails.MessageTemplate4Perform.getPropPswd() +
                  "; numOfParams=" + numOfParams);
          messageDetails.Confirmation.clear();
          messageDetails.XML_MsgResponse.setLength(0);

          StringBuilder queryString= new StringBuilder(messageDetails.XML_MsgSEND.length());

          queryString.append("?queue_id=").append(messageQueueVO.getQueue_Id());
          for (Map.Entry<String, String> entry: HttpGetParams.entrySet()) {
              queryString.append("&");
              queryString.append(entry.getKey()).append("=").append(entry.getValue());

          }
          Escaper restElmntEscaper = UrlEscapers.urlFragmentEscaper();
          restElmntEscaper.escape(queryString.toString());
          URI URI_4_GET = URI.create(EndPointUrl + restElmntEscaper.escape(queryString.toString()));

          if (IsDebugged) {
              MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "HttpGetMessage.GET URI=`" + EndPointUrl + restElmntEscaper.escape(queryString.toString()) + "`");
              ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog(messageQueueVO.getQueue_Id(), queryString.toString(), MessageSend_Log);
          }

          HttpRequest.Builder requestBuilder = java.net.http.HttpRequest.newBuilder();
          if ( (messageDetails.MessageTemplate4Perform.restPasswordAuthenticator != null) &&
                  (messageDetails.MessageTemplate4Perform.getIsPreemptive())  // adding the header to the HttpRequest
          ) {  // добавляем Authorization заголовки через HttpRequest.Builder
              String encodedAuth = Base64.getEncoder()
                      .encodeToString((PropUser + ":" + PropPswd ).getBytes(StandardCharsets.UTF_8));
              requestBuilder = requestBuilder
                      .header("Authorization", "Basic " + encodedAuth );
          }
          java.net.http.HttpRequest request = requestBuilder
                  .GET( )
                  .uri( URI_4_GET)
                  .header("User-Agent", "msgBus/Java-21")
                  .header("Accept", "*/*")
                  .header("Connection", "close")
                  .timeout( Duration.ofSeconds( messageTemplate4Perform.getPropTimeout_Read()) )
                  .build();
          RestResponseGet = ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofString() );
          RestResponse = RestResponseGet.body(); //.toString();
          // messageDetails.SimpleHttpClient.
          restResponseStatus = RestResponseGet.statusCode();

          // -- Задваивается в случае ошибки => это делается внутри ProcessingSendError()
          // messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);
          if (IsDebugged)
              MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "HttpGetMessage.GET RestResponse=(" + RestResponse + ")");
          // MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Unirest.get escapeXml.RestResponse=(" + XML.escape(RestResponse) + ")");

      } catch (Exception e) {
          System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpGetMessage.GET `" + EndPointUrl + "` Exception");
          e.printStackTrace();
          MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "HttpGetMessage fault:" + e);
          messageDetails.MsgReason.append(" HttpGetMessage.GET `" + EndPointUrl + "` fault: ").append(sStackTrace.strInterruptedException(e));
          MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                  "HttpGetMessage.GET", true, e, MessageSend_Log);
          if (IsDebugged)
              theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e), MessageSend_Log);
          return -1;
      }
      if ((messageDetails.MessageTemplate4Perform.getPropEncoding_Out() != null) &&
              (!messageDetails.MessageTemplate4Perform.getPropEncoding_Out().equals("UTF-8"))) {
          try {
              RestResponse = XML.to_UTF_8(RestResponse, messageDetails.MessageTemplate4Perform.getPropEncoding_Out());
          } catch (Exception e) {
              System.err.println("[" + messageQueueVO.getQueue_Id() + "] UnsupportedEncodingException");
              e.printStackTrace();
              MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] from " + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() + " to_UTF_8 fault:" + e.toString());
              messageDetails.MsgReason.append(" HttpGetMessage.get.to_UTF_8 fault: ").append(sStackTrace.strInterruptedException(e));
              MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                      "HttpGetMessage.GET", true, e, MessageSend_Log);
              if (IsDebugged)
                  theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e), MessageSend_Log);
              return -1;
          }
      }
      if (IsDebugged)
          theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse, MessageSend_Log);

      try {
          Document XMLdocument;
          try {
              JSONObject RestResponseJSON = new JSONObject(RestResponse);
              messageDetails.XML_MsgResponse.append(XMLchars.Envelope_Begin);
              messageDetails.XML_MsgResponse.append(XMLchars.Body_Begin);
              XML.setMessege_Log(MessageSend_Log);
              messageDetails.XML_MsgResponse.append(XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse));
              messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
              messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);
              MessageSend_Log.info("HttpGetMessage.GET: Response=(" + messageDetails.XML_MsgResponse.toString() + ")");

              ByteArrayInputStream parsedRestResponseStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
              SAXBuilder documentBuilder = new SAXBuilder();

              XMLdocument = documentBuilder.build(parsedRestResponseStream);
          } catch (JDOMException RestResponseE) {

              System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpGetMessage.JSONObject Exception" + RestResponseE.getMessage());
              RestResponseE.printStackTrace();
              System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpGetMessage.RestResponse[" + RestResponse + "]");
              MessageSend_Log.error("HttpGetMessage.getResponseBody fault: " + sStackTrace.strInterruptedException(RestResponseE));
              XMLdocument = null;
              messageDetails.XML_MsgResponse.setLength(0);
              messageDetails.XML_MsgResponse.trimToSize();
              messageDetails.XML_MsgResponse.append(XMLchars.Fault_ExtResponse_Begin);
              messageDetails.XML_MsgResponse.append(restResponseStatus);
              messageDetails.XML_MsgResponse.append(XMLchars.FaultExtResponse_FaultString);
              messageDetails.XML_MsgResponse.append(RestResponse);
              messageDetails.XML_MsgResponse.append(XMLchars.FaultExtResponse_End);
          }
          // Получили ответ от сервиса, инициируем обработку getResponseBody()

          MessageSoapSend.getResponseBody(messageDetails, XMLdocument, MessageSend_Log);
          MessageSend_Log.info("HttpGetMessage.GET :ClearBodyResponse=(" + messageDetails.XML_ClearBodyResponse.toString() + ")");
          // client.wait(100);

      } catch (Exception e) {
          System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpGetMessage.JSONObject Exception");
          e.printStackTrace();
          System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpGetMessage.RestResponse[" + RestResponse + "]");
          MessageSend_Log.error("HttpGetMessage.getResponseBody fault: " + sStackTrace.strInterruptedException(e));
          messageDetails.MsgReason.append(" HttpGetMessage.getResponseBody fault: ").append(sStackTrace.strInterruptedException(e));

          MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                  "HttpGetMessage.getResponseBody", true, e, MessageSend_Log);
          return -3;
      }
  } // делаем, всё,  что можно и нужно
  catch ( Exception allE) {
      if (ApiRestHttpClient != null)
      try {
          ApiRestHttpClient.close();

      } catch ( Exception IOE ) {
          MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "HttpGetMessage.ApiRestHttpClient.close fault, Exception:" + IOE.getMessage());
      }
      ApiRestHttpClient = null;
      /*
      try {
          syncConnectionManager.shutdown();
          syncConnectionManager.close();
      } catch ( Exception anyE ) {
          MessageSend_Log.error("[" + "]" + "HttpGetMessage.Unirest.syncConnectionManager.close fault, UnirestException:" + anyE);
      }
      syncConnectionManager = null;*/

      } finally {
          MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "]" + "HttpGetMessage.ApiRestHttpClient.close finally" );
          if (ApiRestHttpClient != null)
              try {
                  ApiRestHttpClient.close();

              } catch ( Exception IOE ) {
                  MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "HttpGetMessage.ApiRestHttpClient.close finally fault, Exception:" + IOE.getMessage());
              }
          ApiRestHttpClient = null;
          /*
          if (syncConnectionManager != null)
              try {
                  syncConnectionManager.shutdown();
                  syncConnectionManager.close();
              } catch ( Exception anyE ) {
                  MessageSend_Log.error("[" + "]" + "HttpGetMessage.Unirest.syncConnectionManager.close finally fault, UnirestException:" + anyE);
              }
          syncConnectionManager = null;*/
      }
            return 0;
  }

    private static int setHttpGetParams(long Queue_Id, String xml_msgSEND, HashMap<String, String> paramsInXml, String Encoding_Out, Logger MessageSend_Log)
            throws JDOMException, IOException, XPathExpressionException {
        SAXBuilder documentBuilder = new SAXBuilder();
        //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream parsedConfigStream = new ByteArrayInputStream(xml_msgSEND.getBytes(StandardCharsets.UTF_8));
        Document document =  documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);
        // MessageSend_Log.info( "["+ Queue_Id + "] setHttpGetParams=( xml_msgSEND =>`" + xml_msgSEND + "`");
        Element RestParams = document.getRootElement();
        paramsInXml.clear();
        int nOfParams=0;
        // Escaper restElmntEscaper = UrlEscapers.urlFragmentEscaper();

            // String deftarget = Envelope.getAttributeValue("default", "all");
            List<Element> list = RestParams.getChildren();
            // Перебор всех элементов RestParams
            for (int i = 0; i < list.size(); i++) {
                Element RestElmnt = (Element) list.get(i);
                String RestElmntText = RestElmnt.getText();
                if ( RestElmntText != null && RestElmntText.length() > 0 ) {
                    nOfParams += 1;
                    if (( Encoding_Out != null) && ( !Encoding_Out.equals( "UTF-8" )) )
                        paramsInXml.put(RestElmnt.getName(), XML.from_UTF_8( XML.escape( RestElmnt.getText()), Encoding_Out ) );
                        else {
                            //String RestElmntTest = RestElmnt.getText().replace(" ","%20")
                            //                                          .replace("?", "%3F")
                            //                                          .replace("&", "%26");
                            paramsInXml.put(RestElmnt.getName(), RestElmnt.getText()
                                // restElmntEscaper.escape( RestElmnt.getText() )
                                //URLEncoder.encode(RestElmnt.getText(), StandardCharsets.UTF_8.toString())
                                // XML.escape( RestElmnt.getText() )
                             );
                        }
                    MessageSend_Log.info( "["+ Queue_Id + "] setHttpGetParams=(" + RestElmnt.getName() + "=>`" + paramsInXml.toString() +
                            //URLEncoder.encode( RestElmnt.getText() , StandardCharsets.UTF_8.toString() ) +
                            "`");
                }
            }
        return nOfParams;
    }

    public static String getResponseBody(@NotNull MessageDetails messageDetails, Document p_XMLdocument, Logger MessageSend_Log) throws JDOMException, IOException, XPathExpressionException {

        SAXBuilder documentBuilder;
        InputStream parsedConfigStream;
        Document XMLdocument;
        //  Если прарсинг ответа НЕ прошел, то тут уже псевдо-ответ от обработчика ошибки парсера
        if ( p_XMLdocument == null ) {
            documentBuilder = new SAXBuilder();
            parsedConfigStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
            XMLdocument = documentBuilder.build(parsedConfigStream);
        }
        else //  Прарсинг ответа прошел, используем присланное
            XMLdocument = p_XMLdocument;
		// --//  DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		//InputStream parsedConfigStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
		// Document XMLdocument = documentBuilder.build(parsedConfigStream);

		Element SoapEnvelope = XMLdocument.getRootElement();
		boolean isSoapBodyFinded = false;
		if ( SoapEnvelope.getName().equals(XMLchars.Envelope) ) {
			// String deftarget = Envelope.getAttributeValue("default", "all");
			List<Element> list = SoapEnvelope.getChildren();
			// Перебор всех элементов Envelope
			for (int i = 0; i < list.size(); i++) {
				Element SoapElmnt = (Element) list.get(i);
				if ( SoapElmnt.getName().equals(XMLchars.Body) ) {
					// MessageSend_Log.info("client:getResponseBody=(\n" + SoapElmnt.getName());
					isSoapBodyFinded = true;

					// надо подготовить очищенный от ns: содержимое Body.
					messageDetails.Confirmation.clear();
					messageDetails.XML_ClearBodyResponse.setLength(0);
                    JsonBody2XML_String(messageDetails, SoapElmnt, MessageSend_Log);
				}
			}

			if ( !isSoapBodyFinded )
				throw new XPathExpressionException("getResponseBody: в SOAP-ответе не найден Element=" + XMLchars.Body);

		} else {
			throw new XPathExpressionException("getResponseBody: в SOAP-ответе не найден RootElement=" + XMLchars.Envelope);
		}

		return null;
	}

    public static int JsonBody2XML_String(@NotNull MessageDetails messageDetails, Element SoapBody, Logger MessageSend_Log) {
        MessageDetailVO messageDetailVO = messageDetails.Message.get(0);
        int BodyListSize = 0;
        // LinkedList<MessageDetailVO> linkedTags = new LinkedList<>();
        // linkedTags.clear();
        if ( messageDetailVO.Tag_Num != 0 ) {
            List<Element> list = SoapBody.getChildren();
            // Перебор всех элементов Envelope
            for (int i = 0; i < list.size(); i++) {
                Element SoapElmnt = (Element) list.get(i);
                // MessageSend_Log.info("Rest:JsonBody2XML_String=(\n" + SoapElmnt.getName() + " =" + SoapElmnt.getText() + "\n");
                // надо подготовить очищенный от ns: содержимое Body.
                messageDetails.XML_ClearBodyResponse.append(OpenTag).append( SoapElmnt.getName() ).append( XMLchars.CloseTag);
                MessageSoapSend.XML_BodyElemets2StringB(messageDetails, SoapElmnt, MessageSend_Log); // Рекурсивный вызов для элемента внутри <Body>
                messageDetails.XML_ClearBodyResponse.append(OpenTag).append( XMLchars.EndTag ).append( SoapElmnt.getName() ).append( XMLchars.CloseTag);
                //MessageSend_Log.info(messageDetails.XML_ClearBodyResponse.toString());
            }
            // TODO - что то надо делать когда корневой элемент без дочерних элементов
            if ( list.size() == 0 ) // корневой элемент без дочерних элементов
            {
                messageDetails.XML_ClearBodyResponse.append(OpenTag).append( SoapBody.getName() ).append( XMLchars.CloseTag);
                messageDetails.XML_ClearBodyResponse.append(SoapBody.getText());
                messageDetails.XML_ClearBodyResponse.append(OpenTag).append( XMLchars.EndTag ).append( SoapBody.getName() ).append( XMLchars.CloseTag);
            }

        }
        return BodyListSize;

    }

    public static long WebRestExePostExec(MessageQueueVO messageQueueVO, MessageTemplate4Perform messageTemplate4Perform,
                                          TheadDataAccess theadDataAccess, int ApiRestWaitTime, Logger MessageSend_Log ) {
        String EndPointUrl= null;
        String RestResponse = null;
        int restResponseStatus=0;
        long Queue_Id =  messageQueueVO.getQueue_Id();
        boolean IsDebugged = messageTemplate4Perform.getIsDebugged();
        HttpClient ApiRestHttpClient = null;

        String PropUser = messageTemplate4Perform.getPropUserPostExec();
        String PropPswd = messageTemplate4Perform.getPropPswdPostExec();
        try {
            if ( (messageTemplate4Perform.postExecPasswordAuthenticator != null)
                    // В гермесе БАСИК-Authenticator, пока что!
                && (! messageTemplate4Perform.getPreemptivePostExec())  // adding the header to the HttpRequest and removing Authenticator
            )
            {
                if ( IsDebugged ) {
                    MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] WebRestExePostExec PropUser=`" + PropUser + "` PropPswd=`" + PropPswd + "`");
                }
                ApiRestHttpClient = HttpClient.newBuilder()
                        .authenticator( messageTemplate4Perform.postExecPasswordAuthenticator )
                        .followRedirects(HttpClient.Redirect.ALWAYS)
                        .version(HttpClient.Version.HTTP_1_1)
                        .connectTimeout(Duration.ofSeconds( 5 ))
                        .build();
            }
            else {
                if ( IsDebugged )
                    MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] WebRestExePostExec.4.GET PropUser== null, PreemptivePostExec (`" + messageTemplate4Perform.getPreemptivePostExec() + "`)" );
                ApiRestHttpClient = HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .followRedirects(HttpClient.Redirect.ALWAYS)
                        .connectTimeout(Duration.ofSeconds(5))
                        .build();
            }

            if ( StringUtils.substring(messageTemplate4Perform.getPropHostPostExec(),0,"http".length()).equalsIgnoreCase("http") )
                EndPointUrl = messageTemplate4Perform.getPropHostPostExec() +
                              messageTemplate4Perform.getPropUrlPostExec();
            else
                EndPointUrl = "http://" + messageTemplate4Perform.getPropHostPostExec() +
                                          messageTemplate4Perform.getPropUrlPostExec();


            HttpRequest.Builder requestBuilder = java.net.http.HttpRequest.newBuilder();

            if ( messageTemplate4Perform.getPreemptivePostExec() ) // adding the header to the HttpRequest
             {  // добавляем Authorization заголовки через HttpRequest.Builder
                 String encodedAuth = Base64.getEncoder()
                                     .encodeToString((PropUser + ":" + PropPswd ).getBytes(StandardCharsets.UTF_8));
                requestBuilder = requestBuilder
                        .header("Authorization", "Basic " + encodedAuth );
                 if ( messageTemplate4Perform.getIsDebugged() )
                     MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] Authorization Basic " + encodedAuth +
                                    " (using User=`" + PropUser + "` Pswd=`" + PropPswd + "`)" );
            }

            java.net.http.HttpRequest request = requestBuilder
                    .GET()
                    .uri( URI.create(EndPointUrl + "?queue_id=" + String.valueOf(Queue_Id) ))
                    .header("User-Agent", "msgBus/Java-21")
                    .header("Accept", "*/*")
                    .header("Connection", "close")
                    .timeout( Duration.ofSeconds( ApiRestWaitTime ) )
                    .build();
            HttpResponse<String> RestResponseGet = ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofString() );
            RestResponse = RestResponseGet.body(); //.toString();

            restResponseStatus = RestResponseGet.statusCode(); //500; //RestResponseGet.statusCode();

            if ( messageTemplate4Perform.getIsDebugged() )
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] WebRestExePostExec.GET(" + EndPointUrl + "?queue_id=" + String.valueOf(Queue_Id) + ") httpStatus=[" + restResponseStatus + "] RestResponse=(`" + RestResponse + "`)");

        } catch ( Exception e) {
            // возмущаемся, но оставляем сообщение в ResOUT что бы обработчик в кроне мог доработать
            MessageSend_Log.error("["+ messageQueueVO.getQueue_Id() +"] Ошибка пост-обработки HttpGet(" + EndPointUrl + "?queue_id=" + String.valueOf(Queue_Id) + "), вызов от имени пользователя(`"+ PropUser + "/" + PropPswd + "`):" + e.toString() );
            theadDataAccess.doUPDATE_MessageQueue_SetMsg_Reason(messageQueueVO,
                    "Ошибка пост-обработки HttpGet(" + EndPointUrl + "), вызов от имени пользователя(`"+ PropUser +"`):" + sStackTrace.strInterruptedException(e), 123567,
                    messageQueueVO.getRetry_Count(),  MessageSend_Log);
             if ( ApiRestHttpClient !=null) ApiRestHttpClient.close();
            return -17L;
        }
        try {
            JSONObject RestResponseJSON = new JSONObject( RestResponse );
            MessageSend_Log.info("["+ messageQueueVO.getQueue_Id() + "] WebRestExePostExec=(`" + RestResponseJSON.toString() + "`)");
            ReadContext jsonContext = JsonPath.parse(RestResponse);
            Object msgStatus;
            Object queueDirection;
            String msgResult;
            String sQueueDirection= XMLchars.DirectATTNOUT;
            Integer iMsg_Status = messageQueueVO.getMsg_Status() ;

            try {

                msgStatus = jsonContext.read("$." + messageTemplate4Perform.getPropMsgStatus() );
                queueDirection = jsonContext.read("$." + messageTemplate4Perform.getPropQueueDirection() );
                if (queueDirection.toString().equalsIgnoreCase(XMLchars.DirectDELOUT))
                    sQueueDirection = XMLchars.DirectDELOUT;

                msgResult = jsonContext.read("$." + messageTemplate4Perform.getPropMsgResult() );
            }
            catch ( ClassCastException | InvalidPathException exc) {
                //resultMessage = "-x-x-";
                //resultCode = "-x-";
                theadDataAccess.doUPDATE_MessageQueue_SetMsg_Result(messageQueueVO, sQueueDirection, 0 + iMsg_Status,
                        "Пост-обработчик HttpGet (http="+restResponseStatus + ") вернул по url(" + EndPointUrl + "): JSon `" + RestResponse + "` в котором нет $.msgStatus или $.msgResult" ,
                        MessageSend_Log);
                ApiRestHttpClient.close();
                return -13L;
            }
            try {
            iMsg_Status = ((Integer) msgStatus); }
            catch ( ClassCastException exc) {
                iMsg_Status = messageQueueVO.getMsg_Status() ;
            }

            theadDataAccess.doUPDATE_MessageQueue_SetMsg_Result(messageQueueVO, sQueueDirection, 0 + iMsg_Status,
                    "Пост-обработчик HttpGet (http="+restResponseStatus + ") вернул JSon(" + EndPointUrl + "):" + msgStatus.toString() + " Message:" + msgResult,
                    MessageSend_Log);

        } catch (JSONException | InvalidJsonException  e) {

            MessageSend_Log.warn("["+ messageQueueVO.getQueue_Id() +"] Пост-обработчик HttpGet не вернул JSon(" + EndPointUrl + "):" + e.toString() );
            theadDataAccess.do_SelectMESSAGE_QUEUE(  messageQueueVO, MessageSend_Log );
            theadDataAccess.doUPDATE_MessageQueue_SetMsg_Result(messageQueueVO, messageQueueVO.getQueue_Direction(), 0 + messageQueueVO.getMsg_Status() ,
                    "Пост-обработчик HttpGet (http="+restResponseStatus + ") не вернул JSon(" + EndPointUrl + "):`" + RestResponse + "` " + messageQueueVO.getMsg_Result(),
                     MessageSend_Log);
            ApiRestHttpClient.close();
            return 0L;
        }
        ApiRestHttpClient.close();
        return 0L;

    }

    public static long WebRestErrOUTPostExec(MessageQueueVO messageQueueVO, MessageTemplate4Perform messageTemplate4Perform, int ApiRestWaitTime,
                                             TheadDataAccess theadDataAccess,  Logger MessageSend_Log ) {
        String EndPointUrl= null;
        String RestResponse = null;
        int restResponseStatus=0;
        long Queue_Id =  messageQueueVO.getQueue_Id();
        boolean IsDebugged = messageTemplate4Perform.getIsDebugged();
        HttpClient ApiRestHttpClient=null;
        String PropUser = messageTemplate4Perform.getPropUserPostExec();
        String PropPswd = messageTemplate4Perform.getPropPswdPostExec();
        try {

            if ( (messageTemplate4Perform.postExecPasswordAuthenticator != null)
                  //  && (!messageTemplate4Perform.getIsPreemptive())  // adding the header to the HttpRequest and removing Authenticator
            )
            {
                if ( IsDebugged ) {
                    MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] WebRestExePostExec.GET PropUser=`" + PropUser + "` PropPswd=`" + PropPswd + "`");
                }
                ApiRestHttpClient = HttpClient.newBuilder()
                        .authenticator( messageTemplate4Perform.postExecPasswordAuthenticator )
                        .followRedirects(HttpClient.Redirect.ALWAYS)
                        .version(HttpClient.Version.HTTP_1_1)
                        .connectTimeout(Duration.ofSeconds( 5 ))
                        .build();
            }
            else {
                if ( IsDebugged )
                    MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] WebRestExePostExec.GET PropUser== null (`" + PropUser + "`)" );
                ApiRestHttpClient = HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .followRedirects(HttpClient.Redirect.ALWAYS)
                        .connectTimeout(Duration.ofSeconds(5))
                        .build();
            }

            if ( StringUtils.substring(messageTemplate4Perform.getPropHostPostExec(),0,"http".length()).equalsIgnoreCase("http") )
                EndPointUrl =   messageTemplate4Perform.getPropHostPostExec() +
                                messageTemplate4Perform.getPropUrlPostExec();
            else
                EndPointUrl = "http://" + messageTemplate4Perform.getPropHostPostExec() +
                                          messageTemplate4Perform.getPropUrlPostExec();

            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                    .GET()
                    .uri( URI.create(EndPointUrl + "?queue_id=" + String.valueOf(Queue_Id) ))
                    .header("User-Agent", "msgBus/Java-21")
                    .header("Accept", "*/*")
                    .header("Connection", "close")
                    .timeout( Duration.ofSeconds( ApiRestWaitTime ) )
                    .build();
            HttpResponse<String> RestResponseGet = ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofString() );
            RestResponse = RestResponseGet.body(); //.toString();

            //RestResponse =  RestResponseGet.getBody().toString(); // "{\"error\": \"Unirest disabled!\"}" ; //
            restResponseStatus = RestResponseGet.statusCode(); //500; //RestResponseGet.statusCode();

            if ( IsDebugged )
                MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] WebRestErrOUTPostExec.get(" + EndPointUrl + ") httpStatus=[" + restResponseStatus + "] RestResponse=(`" + RestResponse + "`)");

        } catch ( Exception e) {
            // возмущаемся, но оставляем сообщение в ResOUT что бы обработчик в кроне мог доработать
            MessageSend_Log.error("["+ messageQueueVO.getQueue_Id() +"] Ошибка пост-обработки WebRestErrOUTPostExec HttpGet(" + EndPointUrl + "):" + e.toString() );
            theadDataAccess.doUPDATE_MessageQueue_SetMsg_Reason(messageQueueVO,
                    "Ошибка пост-обработки HttpGet(" + EndPointUrl + "):" + sStackTrace.strInterruptedException(e), 135699,
                    messageQueueVO.getRetry_Count(),  MessageSend_Log);
            if ( ApiRestHttpClient !=null) ApiRestHttpClient.close();
            return -171L;
        }
        ApiRestHttpClient.close();
        return 0L;

    }


}
