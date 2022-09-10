package net.plumbing.msgbus.threads.utils;

import com.google.common.collect.ImmutableMap;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import net.plumbing.msgbus.model.*;
import net.plumbing.msgbus.threads.TheadDataAccess;
import org.apache.commons.io.IOUtils;
/*
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
*/
//import oracle.jdbc.internal.OracleRowId;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;
import org.slf4j.Logger;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.sStackTracе;
import net.plumbing.msgbus.monitoring.ConcurrentQueue;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SSLContext;
import javax.security.cert.CertificateException;
import javax.security.cert.X509Certificate;

import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;

import javax.validation.constraints.NotNull;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.RowId;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;

import static net.plumbing.msgbus.common.XMLchars.OpenTag;


public class MessageHttpSend {

    public static SSLContext getSSLContext() {
        SSLContext sslContext;
        try {
             sslContext = new SSLContextBuilder()
                     .loadTrustMaterial(null, new TrustSelfSignedStrategy() {
                public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    return true;
                }
            }).build();
        } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
            e.printStackTrace();
            sslContext=null;
        }
        return sslContext;
    }

    public static int sendSoapMessage(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess, MonitoringQueueVO monitoringQueueVO, Logger MessegeSend_Log) {
        //
        StringBuilder SoapEnvelope = new StringBuilder(XMLchars.Envelope_Begin);
        if ( messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty) >= 0 )
            // Header_is_empty !
            SoapEnvelope.append(XMLchars.Empty_Header);
        else {
            SoapEnvelope.append(XMLchars.Header_Begin);
            SoapEnvelope.append(messageDetails.Soap_HeaderRequest);
            SoapEnvelope.append(XMLchars.Header_End);
        }
        SoapEnvelope.append(XMLchars.Body_Begin);
        SoapEnvelope.append(messageDetails.XML_MsgSEND);
        SoapEnvelope.append(XMLchars.Body_End);
        SoapEnvelope.append(XMLchars.Envelope_End);

        MessageTemplate4Perform messageTemplate4Perform = messageDetails.MessageTemplate4Perform;

        String EndPointUrl;
        if ( StringUtils.substring(messageTemplate4Perform.getEndPointUrl(),0,"http".length()).equalsIgnoreCase("http") )
            EndPointUrl = messageTemplate4Perform.getEndPointUrl();
        else
            EndPointUrl = "http://" + messageTemplate4Perform.getEndPointUrl();

        int  ConnectTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Conn() * 1000;
        int ReadTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Read() * 1000;

        // TODO : for Ora RowId ROWID_QUEUElog=null;
        String ROWID_QUEUElog=null;
        String RestResponse=null;
        InputStream Response;
        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());

        byte[] RequestBody;
        if (( messageDetails.MessageTemplate4Perform.getPropEncoding_Out() !=null ) &&
                (messageDetails.MessageTemplate4Perform.getPropEncoding_Out() != "UTF-8" )) {
            try {
                RequestBody = SoapEnvelope.toString().getBytes( messageDetails.MessageTemplate4Perform.getPropEncoding_Out());
            } catch (UnsupportedEncodingException e) {
                System.err.println("[" + messageQueueVO.getQueue_Id() + "] UnsupportedEncodingException");
                e.printStackTrace();
                MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] from " + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() + " to_UTF_8 fault:" + e.toString());
                messageDetails.MsgReason.append(" HttpGetMessage.post.to" + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() +  " fault: " + sStackTracе.strInterruptedException(e));
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "HttpGetMessage.Unirest.post", true, e, MessegeSend_Log);
                ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, //SoapEnvelope.toString(),
                        null, //" HttpGetMessage.post.to" + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() +  " fault: ",
                        monitoringQueueVO, MessegeSend_Log);
                return -1;
            }
        }
        else
            RequestBody = SoapEnvelope.toString().getBytes ( StandardCharsets.UTF_8 );

        try {
            //  устанавливаем "своего" HttpClient с предварительно выставленными тайм-аутами из шаблона и SSL
            Unirest.setHttpClient( messageDetails.SimpleHttpClient);

            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
            MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.Unirest.post(" + EndPointUrl + ").connectTimeoutInMillis=" + ConnectTimeoutInMillis +
                    ";.readTimeoutInMillis=" + ReadTimeoutInMillis +
                    ";.PropUser=" + messageDetails.MessageTemplate4Perform.getPropUser() +
                    ";.PropPswd=" + messageDetails.MessageTemplate4Perform.getPropPswd() +
                    ";."+ messageDetails.MessageTemplate4Perform.SOAP_ACTION_11 + "=" + messageDetails.MessageTemplate4Perform.getSOAPAction()
            );
            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
            MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.Unirest.post[" + SoapEnvelope.toString() + "]" );
                    messageDetails.Confirmation.clear();
            messageDetails.XML_MsgResponse.setLength(0);
            String PropUser = messageDetails.MessageTemplate4Perform.getPropUser();

            String SOAPAction=messageDetails.MessageTemplate4Perform.getSOAPAction();
            if ( SOAPAction == null)
                SOAPAction= "";
            // InputStream parsedMessageStream = new ByteArrayInputStream(SoapEnvelope.toString().getBytes(StandardCharsets.UTF_8));
            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
                ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog( messageQueueVO.getQueue_Id(), SoapEnvelope.toString(), MessegeSend_Log );

            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, //SoapEnvelope.toString(),
                    null, monitoringQueueVO, MessegeSend_Log);

            if ( PropUser != null  ) {
                if ( SOAPAction != null)
                    Response =
                        Unirest.post(EndPointUrl)
                                .header("Content-Type", "text/xml;charset=UTF-8")
                                .header(messageDetails.MessageTemplate4Perform.SOAP_ACTION_11,SOAPAction)
                                .header("User-Agent", "Hermes/Java-11")
                                .header("Accept", "*/*")
                                .basicAuth(PropUser, messageDetails.MessageTemplate4Perform.getPropPswd())
                                .body(RequestBody)
                                .asBinary()
                                .getRawBody();
                else
                    Response =
                            Unirest.post(EndPointUrl)
                            .header("Content-Type", "text/xml;charset=UTF-8")
                            .basicAuth(PropUser, messageDetails.MessageTemplate4Perform.getPropPswd())
                            .body(RequestBody)
                            .asBinary()
                            .getRawBody();
            }
            else {
                if ( SOAPAction != null)
                    Response =
                        Unirest.post(EndPointUrl)
                                .header("Content-Type", "text/xml;charset=UTF-8")
                                .header("User-Agent", "Hermes/Java-11")
                                .header("Accept", "*/*")
                                .header(messageDetails.MessageTemplate4Perform.SOAP_ACTION_11,SOAPAction)
                                .body(RequestBody)
                                .asBinary()
                                .getRawBody();
                else
                    Response =
                            Unirest.post(EndPointUrl)
                            .header("Content-Type", "text/xml;charset=UTF-8")
                            .body(RequestBody)
                            .asBinary()
                            .getBody();
            }


            // перекодируем ответ из кодировки, которая была указана в шаблоне для внешней системы в UTF_8
            // Response => RestResponse;
            try {
                if (( messageDetails.MessageTemplate4Perform.getPropEncoding_Out() !=null ) &&
                        (messageDetails.MessageTemplate4Perform.getPropEncoding_Out() != "UTF-8" )) {
                    RestResponse = IOUtils.toString(Response, messageDetails.MessageTemplate4Perform.getPropEncoding_Out() ); //StandardCharsets.UTF_8);
                }
                else RestResponse = IOUtils.toString(Response, StandardCharsets.UTF_8);
            }
            catch (Exception e) {
                System.err.println( "["+ messageQueueVO.getQueue_Id()  + "] IOUtils.toString.UnsupportedEncodingException" );
                e.printStackTrace();
                MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] IOUtils.toString from " +
                        messageDetails.MessageTemplate4Perform.getPropEncoding_Out() ==  null ? "UTF_8" : messageDetails.MessageTemplate4Perform.getPropEncoding_Out()
                        + " to_UTF_8 fault:" + e.toString() );
                messageDetails.MsgReason.append(" HttpGetMessage.post.to_UTF_8 fault: " + sStackTracе.strInterruptedException(e));
                MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                        "HttpGetMessage.Unirest.post", true,  e ,  MessegeSend_Log);
                ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, SoapEnvelope.toString(),
                //        "HttpGetMessage.post.to_UTF_8 fault: " + sStackTracе.strInterruptedException(e), monitoringQueueVO, MessegeSend_Log);
                return -1;
            }

//            сохраняем в XML_MsgResponse SOAP-конверт уже в UTF_8
            messageDetails.XML_MsgResponse.append( RestResponse ); // cj,cn

            // -- Задваивается в случае ошибки => это делается внутри ProcessingSendError()
            // messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);
            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
            MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.Unirest.Response=(" + messageDetails.XML_MsgResponse.toString() + ")");
            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
                theadDataAccess.doUPDATE_QUEUElog( ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse, MessegeSend_Log );

        } catch ( UnirestException e) {
            // Журналируем ответ как есть
            MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.Unirest.post ("+EndPointUrl+") fault:" + e.toString() );
            messageDetails.MsgReason.append(" sendSoapMessage.Unirest.post (" + EndPointUrl + ") fault: " + sStackTracе.strInterruptedException(e));

            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
                theadDataAccess.doUPDATE_QUEUElog( ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTracе.strInterruptedException(e), MessegeSend_Log );

            // HE-4892 Если транспорт отвалился , то Шина ВСЁ РАВНО формирует как бы ответ , но с Fault внутри.
            // НАДО проверять количество порыток !!!
            MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Retry_Count ("+messageQueueVO.getRetry_Count()+")>= " +
                    "( ShortRetryCount=" +messageDetails.MessageTemplate4Perform.getShortRetryCount() +
                    " LongRetryCount=" + messageDetails.MessageTemplate4Perform.getLongRetryCount() + ")" );
            if ( messageQueueVO.getRetry_Count() +1  >= messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount() ) {
                // количество порыток исчерпано, формируем результат для выхода из повторов
                MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage.Unirest.post (" + EndPointUrl + ") fault:" + e.toString());
                messageDetails.XML_MsgResponse.setLength(0);
                messageDetails.XML_MsgResponse.append(XMLchars.Envelope_Begin);
                messageDetails.XML_MsgResponse.append(XMLchars.Body_Begin);
                messageDetails.XML_MsgResponse.append(XMLchars.Fault_Begin);
                messageDetails.XML_MsgResponse.append("sendSoapMessage (" + EndPointUrl + ") fault:" + e.toString());
                messageDetails.XML_MsgResponse.append(XMLchars.Fault_End);
                messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
                messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);

                MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                        "sendSoapMessage.Unirest.post (" + EndPointUrl + ") ", false,  e ,  MessegeSend_Log);
                ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, SoapEnvelope.toString(), messageDetails.XML_MsgResponse.toString(), monitoringQueueVO, MessegeSend_Log);
            }
            else {
                // HE-4892 Если транспорт отвалился , то Шина выставляет RESOUT - коммент ProcessingSendError & return -1;
                 MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                        "sendSoapMessage.Unirest.post (" + EndPointUrl + ") ", true,  e ,  MessegeSend_Log);
                ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, SoapEnvelope.toString(),
                //        "sendSoapMessage.Unirest.post (" + EndPointUrl + ") " + sStackTracе.strInterruptedException(e), monitoringQueueVO, MessegeSend_Log);
                 return -1;
            }
        }
        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, SoapEnvelope.toString(), messageDetails.XML_MsgResponse.toString(), monitoringQueueVO, MessegeSend_Log);
        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);

        try {
            // Получили ответ от сервиса, инициируем обработку SOAP getResponseBody()
            MessageSoapSend.getResponseBody (messageDetails, MessegeSend_Log);
            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
            MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage:ClearBodyResponse=(" + messageDetails.XML_ClearBodyResponse.toString() + ")");
            else {// HE-9187
                if (messageDetails.XML_ClearBodyResponse.length() > 2049)
                    MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage:ClearBodyResponse[2048 char]=(" +
                            messageDetails.XML_ClearBodyResponse.substring(0, 2048)
                            + "...)");
                else
                    MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "sendSoapMessage:ClearBodyResponse[all char]=(" +
                            messageDetails.XML_ClearBodyResponse.toString() + "...)");
                // client.wait(2048); --HE-10763 : Расширить размер логируемого сообщения ( ответ при ответ при сбое на стороне получателя ) до 2 кб
            }

        } catch (Exception e) {
            MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] Retry_Count=" + messageQueueVO.getRetry_Count() + "SendSoapMessage.getResponseBody fault(" + RestResponse + " : " + sStackTracе.strInterruptedException(e));
            messageDetails.MsgReason.append(" sendSoapMessage.getResponseBody fault: " + sStackTracе.strInterruptedException(e));

            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "sendSoapMessage.getResponseBody" , true,  e ,  MessegeSend_Log);
            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, SoapEnvelope.toString(),
            //        "sendSoapMessage.Unirest.post (" + EndPointUrl + ") " + sStackTracе.strInterruptedException(e),monitoringQueueVO, MessegeSend_Log);
            return -3;
        }
        // когда всё хорошо, увеличивать счётчик нет смысла
        // messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);
        return 0;
    }

    public static int sendPostMessage(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess, Logger MessegeSend_Log) {
        //
        MessageTemplate4Perform messageTemplate4Perform = messageDetails.MessageTemplate4Perform;

        String EndPointUrl;
        if ( StringUtils.substring(messageTemplate4Perform.getEndPointUrl(),0,"http".length()).equalsIgnoreCase("http") )
            EndPointUrl = messageTemplate4Perform.getEndPointUrl();
        else
            EndPointUrl = "http://" + messageTemplate4Perform.getEndPointUrl();

        Integer ConnectTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Conn() * 1000;
        Integer ReadTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Read() * 1000;
        String RestResponse=null;
        InputStream Response;
        byte[] RequestBody;

        if (( messageDetails.MessageTemplate4Perform.getPropEncoding_Out() !=null ) &&
                (messageDetails.MessageTemplate4Perform.getPropEncoding_Out() != "UTF-8" )) {
            try {
                RequestBody = messageDetails.XML_MsgSEND.getBytes( messageDetails.MessageTemplate4Perform.getPropEncoding_Out());
            } catch (UnsupportedEncodingException e) {
                System.err.println("[" + messageQueueVO.getQueue_Id() + "] UnsupportedEncodingException");
                e.printStackTrace();
                MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] from " + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() + " to_UTF_8 fault:" + e.toString());
                messageDetails.MsgReason.append(" HttpGetMessage.post.to" + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() +  " fault: " + sStackTracе.strInterruptedException(e));
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "HttpGetMessage.Unirest.post", true, e, MessegeSend_Log);
                return -1;
            }
        }
        else
            RequestBody = messageDetails.XML_MsgSEND.getBytes ( StandardCharsets.UTF_8 );

        try {
            Unirest.setHttpClient( messageDetails.SimpleHttpClient);

            MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Unirest.post(" + EndPointUrl + ").connectTimeoutInMillis=" + ConnectTimeoutInMillis +
                    ";.readTimeoutInMillis=ReadTimeoutInMillis= " + ReadTimeoutInMillis);
            messageDetails.Confirmation.clear();
            messageDetails.XML_MsgResponse.setLength(0);
            String PropUser = messageDetails.MessageTemplate4Perform.getPropUser();

            if ( PropUser != null  )
               Response =
                    Unirest.post(EndPointUrl)
                            .basicAuth(PropUser, messageDetails.MessageTemplate4Perform.getPropPswd())
                            .body( RequestBody )
                            .asBinary()
                            .getRawBody();
            else
                Response =
                        Unirest.post(EndPointUrl)
                                .body( RequestBody )
                                .asBinary()
                                .getRawBody(); //.asString() //.getBody();

            messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);

        } catch ( UnirestException e) {
            System.err.println( "["+ messageQueueVO.getQueue_Id()  + "]  Exception" );
            e.printStackTrace();
            MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Unirest.post ("+EndPointUrl+") fault:" + e.toString() );
            messageDetails.MsgReason.append(" sendPostMessage.Unirest.post fault: " + sStackTracе.strInterruptedException(e));
            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "sendPostMessage.Unirest.post("+EndPointUrl+")", true,  e ,  MessegeSend_Log);
            return -1;
        }
        // перекодируем ответ из кодировки, которая была указана в шаблоне для внешней системы в UTF_8
        try {
        if (( messageDetails.MessageTemplate4Perform.getPropEncoding_Out() !=null ) &&
                (messageDetails.MessageTemplate4Perform.getPropEncoding_Out() != "UTF-8" )) {
             RestResponse = IOUtils.toString(Response, messageDetails.MessageTemplate4Perform.getPropEncoding_Out() ); //StandardCharsets.UTF_8);
        }
        else RestResponse = IOUtils.toString(Response, StandardCharsets.UTF_8);
        }
        catch (Exception e) {
            System.err.println( "["+ messageQueueVO.getQueue_Id()  + "] IOUtils.toString.UnsupportedEncodingException" );
            e.printStackTrace();
            MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] IOUtils.toString from " +
                    messageDetails.MessageTemplate4Perform.getPropEncoding_Out() ==  null ? "UTF_8" : messageDetails.MessageTemplate4Perform.getPropEncoding_Out()
                    + " to_UTF_8 fault:" + e.toString() );
            messageDetails.MsgReason.append(" HttpGetMessage.post.to_UTF_8 fault: " + sStackTracе.strInterruptedException(e));
            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "HttpGetMessage.Unirest.post", true,  e ,  MessegeSend_Log);
            return -1;
        }

        if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
            MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Unirest.post RestResponse=(" + RestResponse + ")");

        //  формируем в XML_MsgResponse ответ а-ля SOAP
        try {
            messageDetails.XML_MsgResponse.append(XMLchars.Envelope_Begin);
            messageDetails.XML_MsgResponse.append(XMLchars.Body_Begin);

            if ( RestResponse.startsWith( "<?xml") || RestResponse.startsWith( "<?XML") )
            {
                int index2 = RestResponse.indexOf("?>"); //6
                messageDetails.XML_MsgResponse.append(RestResponse.substring( index2 + 2 ) );
            }
            else {
                if ( RestResponse.startsWith("{") ) { // Разбираем Json
                    JSONObject RestResponseJSON = new JSONObject( RestResponse );
                    messageDetails.XML_MsgResponse.append( XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse ) );

                } else { // Кладем полученный ответ в <MsgData><![CDATA[" RestResponse "]]</MsgData>
                    messageDetails.XML_MsgResponse.append(XMLchars.OpenTag + XMLchars.NameRootTagContentJsonResponse + XMLchars.CloseTag + XMLchars.CDATAopen );
                    messageDetails.XML_MsgResponse.append(RestResponse);
                    messageDetails.XML_MsgResponse.append(XMLchars.OpenTag + XMLchars.EndTag + XMLchars.NameRootTagContentJsonResponse + XMLchars.CloseTag + XMLchars.CDATAclose );
                }
            }

            messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
            messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);
            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
            MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Unirest.post=(" + messageDetails.XML_MsgResponse.toString() + ")");
            // Получили ответ от сервиса, инициируем обработку getResponseBody()
            getResponseBody(messageDetails, MessegeSend_Log);
            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
            MessegeSend_Log.info("Unirest.post:ClearBodyResponse=(" + messageDetails.XML_ClearBodyResponse.toString() + ")");
            // client.wait(100);

        } catch (Exception e) {
            System.err.println( "["+ messageQueueVO.getQueue_Id()  + "]  Exception" );
            e.printStackTrace();
            MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.getResponseBody fault: " + sStackTracе.strInterruptedException(e));
            messageDetails.MsgReason.append(" sendPostMessage.getResponseBody fault: " + sStackTracе.strInterruptedException(e));

            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "sendPostMessage.getResponseBody", true, e ,  MessegeSend_Log);
            return -3;
        }

        return 0;
    }

///////////////////////////////////// HttpGetMessage //////////////////////////////////////////////////////////////////////////////////////////////////////

    public static int HttpGetMessage(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess, Logger MessegeSend_Log) {

		MessageTemplate4Perform messageTemplate4Perform = messageDetails.MessageTemplate4Perform;
		String EndPointUrl;
            if ( StringUtils.substring(messageTemplate4Perform.getEndPointUrl(),0,"http".length()).equalsIgnoreCase("http") )
                EndPointUrl = messageTemplate4Perform.getEndPointUrl();
            else
                EndPointUrl = "http://" + messageTemplate4Perform.getEndPointUrl();

		Integer ConnectTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Conn() * 1000;
		Integer ReadTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Read() * 1000;
        String RestResponse=null;

        HashMap<String, String > HttpGetParams = new HashMap<String, String >();

        int numOfParams;
        try {
            numOfParams = setHttpGetParams(messageDetails.XML_MsgSEND, HttpGetParams, messageDetails.MessageTemplate4Perform.getPropEncoding_Out(), MessegeSend_Log );
        }
        catch ( Exception e) {
            if (e instanceof UnsupportedEncodingException ) {
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "HttpGetMessage.setHttpGetParams() [" + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() + " не поддерживается]", true, e, MessegeSend_Log);
            }
            else
            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "HttpGetMessage.setHttpGetParams() [не содержит параметров для HtthGet]", true,  e ,  MessegeSend_Log);
            return -1;
        }
        if ( numOfParams < 1) {
            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "HttpGetMessage.setHttpGetParams() [не содержит параметров для HtthGet]", true,  null ,  MessegeSend_Log);
            return -1;
        }
        //  TODO for Oracle ROWID, в случае Postgree String :
        // RowId ROWID_QUEUElog=null;
        String ROWID_QUEUElog=null;
		try {
            Unirest.setHttpClient( messageDetails.SimpleHttpClient);

			MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" + "HttpGetMessage.Unirest.get(" + EndPointUrl + ").connectTimeoutInMillis=" + ConnectTimeoutInMillis +
					";.readTimeoutInMillis=ReadTimeoutInMillis= " + ReadTimeoutInMillis +
                    "; User=" + messageDetails.MessageTemplate4Perform.getPropUser() +
                    "; Pswd" + messageDetails.MessageTemplate4Perform.getPropPswd());
			messageDetails.Confirmation.clear();
			messageDetails.XML_MsgResponse.setLength(0);

            String PropUser = messageDetails.MessageTemplate4Perform.getPropUser();


            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
                ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog( messageQueueVO.getQueue_Id(), HttpGetParams.toString(), MessegeSend_Log );

            // Map<String, Object> stringObjectMap = new Map< String, String >();
            if ( PropUser != null )
             RestResponse =	Unirest.get(EndPointUrl)
                         // .header("Accept", "application/json,text/html,application/xhtml+xml,application/xml;*/*")
						.queryString("queue_id", messageQueueVO.getQueue_Id() )
                        .queryString( ImmutableMap.copyOf( HttpGetParams) )
						.basicAuth(PropUser, messageDetails.MessageTemplate4Perform.getPropPswd())
						.asString().getBody();
            else // Без basicAuth !
                RestResponse = Unirest.get(EndPointUrl)
                                .queryString("queue_id", messageQueueVO.getQueue_Id() )
                                .queryString( ImmutableMap.copyOf( HttpGetParams) )
                                .asString().getBody();


			messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);
            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
                MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" +"sendGetMessage.Unirest.get RestResponse=(" + RestResponse + ")");
            // MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Unirest.get escapeXml.RestResponse=(" + XML.escape(RestResponse) + ")");

		} catch ( UnirestException e) {
            System.err.println( "["+ messageQueueVO.getQueue_Id()  + "] HttpGetMessage.Get Exception" );
            e.printStackTrace();
			MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "HttpGetMessage fault:" + e.toString() );
			messageDetails.MsgReason.append(" HttpGetMessage.Unirest.get fault: " + sStackTracе.strInterruptedException(e));
            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "HttpGetMessage.Unirest.get", true,  e ,  MessegeSend_Log);
            if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
                theadDataAccess.doUPDATE_QUEUElog( ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTracе.strInterruptedException(e), MessegeSend_Log );
			return -1;
		}
		if (( messageDetails.MessageTemplate4Perform.getPropEncoding_Out() !=null ) &&
            (messageDetails.MessageTemplate4Perform.getPropEncoding_Out() != "UTF-8" )) {
		    try {
                RestResponse = XML.to_UTF_8(RestResponse , messageDetails.MessageTemplate4Perform.getPropEncoding_Out() );
            }
            catch (Exception e) {
                System.err.println( "["+ messageQueueVO.getQueue_Id()  + "] UnsupportedEncodingException" );
                e.printStackTrace();
                MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] from " + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() + " to_UTF_8 fault:" + e.toString() );
                messageDetails.MsgReason.append(" HttpGetMessage.get.to_UTF_8 fault: " + sStackTracе.strInterruptedException(e));
                MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                        "HttpGetMessage.Unirest.get", true,  e ,  MessegeSend_Log);
                if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
                    theadDataAccess.doUPDATE_QUEUElog( ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTracе.strInterruptedException(e), MessegeSend_Log );
                return -1;
            }
        }
        if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
            theadDataAccess.doUPDATE_QUEUElog( ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse, MessegeSend_Log );

		try {
            JSONObject RestResponseJSON = new JSONObject( RestResponse );
            messageDetails.XML_MsgResponse.append(XMLchars.Envelope_Begin);
            messageDetails.XML_MsgResponse.append(XMLchars.Body_Begin);
            XML.setMessege_Log( MessegeSend_Log );
            messageDetails.XML_MsgResponse.append( XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse ) );
            messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
            messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);
            MessegeSend_Log.info("client.post:Response=(" + messageDetails.XML_MsgResponse.toString() + ")");
			// Получили ответ от сервиса, инициируем обработку getResponseBody()
			getResponseBody(messageDetails, MessegeSend_Log);
			MessegeSend_Log.info("client.post:ClearBodyResponse=(" + messageDetails.XML_ClearBodyResponse.toString() + ")");
			// client.wait(100);

		} catch (Exception e) {
            System.err.println( "["+ messageQueueVO.getQueue_Id()  + "] HttpGetMessage.JSONObject Exception" );
            e.printStackTrace();
            System.err.println( "["+ messageQueueVO.getQueue_Id()  + "] HttpGetMessage.RestResponse[" + RestResponse + "]" );
			MessegeSend_Log.error("HttpGetMessage.getResponseBody fault: " + sStackTracе.strInterruptedException(e));
			messageDetails.MsgReason.append(" HttpGetMessage.getResponseBody fault: " + sStackTracе.strInterruptedException(e));

            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "HttpGetMessage.getResponseBody", true, e ,  MessegeSend_Log);
			return -3;
		}

		return 0;
	}

    private static int setHttpGetParams(String xml_msgSEND, HashMap<String, String> papamsInXml, String Encoding_Out, Logger MessegeSend_Log)
            throws JDOMException, IOException, XPathExpressionException {
        SAXBuilder documentBuilder = new SAXBuilder();
        //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream parsedConfigStream = new ByteArrayInputStream(xml_msgSEND.getBytes(StandardCharsets.UTF_8));
        Document document = (Document) documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);

        Element RestParams = document.getRootElement();
        papamsInXml.clear();
        int nOfParams=0;

            // String deftarget = Envelope.getAttributeValue("default", "all");
            List<Element> list = RestParams.getChildren();
            // Перебор всех элементов RestParams
            for (int i = 0; i < list.size(); i++) {
                Element RestElmnt = (Element) list.get(i);
                String RestElmntText = RestElmnt.getText();
                if ( RestElmntText != null && RestElmntText.length() > 0 ) {
                    nOfParams += 1;
                    if (( Encoding_Out != null) && ( Encoding_Out!= "UTF-8" ) )
                        papamsInXml.put(RestElmnt.getName(), XML.from_UTF_8( XML.escape( RestElmnt.getText()), Encoding_Out ) );
                        else
                         papamsInXml.put(RestElmnt.getName(), URLEncoder.encode( RestElmnt.getText() , StandardCharsets.UTF_8.toString() )
                            // XML.escape( RestElmnt.getText() )
                              );
                    MessegeSend_Log.info("setHttpGetParams=(" + RestElmnt.getName() + "=>=" + URLEncoder.encode( RestElmnt.getText() , StandardCharsets.UTF_8.toString() )+ ")");
                }
            }
        return nOfParams;
    }

    private static String getResponseBody(@NotNull MessageDetails messageDetails, Logger MessegeSend_Log) throws JDOMException, IOException, XPathExpressionException {
		SAXBuilder documentBuilder = new SAXBuilder();
		//DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		InputStream parsedConfigStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
		Document document = (Document) documentBuilder.build(parsedConfigStream);

		Element SoapEnvelope = document.getRootElement();
		boolean isSoapBodyFinded = false;
		if ( SoapEnvelope.getName().equals(XMLchars.Envelope) ) {
			// String deftarget = Envelope.getAttributeValue("default", "all");
			List<Element> list = SoapEnvelope.getChildren();
			// Перебор всех элементов Envelope
			for (int i = 0; i < list.size(); i++) {
				Element SoapElmnt = (Element) list.get(i);
				if ( SoapElmnt.getName().equals(XMLchars.Body) ) {
					// MessegeSend_Log.info("client:getResponseBody=(\n" + SoapElmnt.getName());
					isSoapBodyFinded = true;

					// надо подготовить очищенный от ns: содержимое Body.
					messageDetails.Confirmation.clear();
					messageDetails.XML_ClearBodyResponse.setLength(0);
                    JsonBody2XML_String(messageDetails, SoapElmnt, MessegeSend_Log);
				}
			}

			if ( !isSoapBodyFinded )
				throw new XPathExpressionException("getResponseBody: в SOAP-ответе не найден Element=" + XMLchars.Body);

		} else {
			throw new XPathExpressionException("getResponseBody: в SOAP-ответе не найден RootElement=" + XMLchars.Envelope);
		}

		return null;
	}

    public static int JsonBody2XML_String(@NotNull MessageDetails messageDetails, Element SoapBody, Logger MessegeSend_Log) {
        MessageDetailVO messageDetailVO = messageDetails.Message.get(0);
        int BodyListSize = 0;
        // LinkedList<MessageDetailVO> linkedTags = new LinkedList<>();
        // linkedTags.clear();
        if ( messageDetailVO.Tag_Num != 0 ) {
            List<Element> list = SoapBody.getChildren();
            // Перебор всех элементов Envelope
            for (int i = 0; i < list.size(); i++) {
                Element SoapElmnt = (Element) list.get(i);
                // MessegeSend_Log.info("Rest:JsonBody2XML_String=(\n" + SoapElmnt.getName() + " =" + SoapElmnt.getText() + "\n");
                // надо подготовить очищенный от ns: содержимое Body.
                messageDetails.XML_ClearBodyResponse.append(OpenTag + SoapElmnt.getName() + XMLchars.CloseTag);
                MessageSoapSend.XML_BodyElemets2StringB(messageDetails, SoapElmnt, MessegeSend_Log);
                messageDetails.XML_ClearBodyResponse.append(OpenTag + XMLchars.EndTag + SoapElmnt.getName() + XMLchars.CloseTag);
                MessegeSend_Log.info(messageDetails.XML_ClearBodyResponse.toString());
            }
        }
        return BodyListSize;

    }



}
