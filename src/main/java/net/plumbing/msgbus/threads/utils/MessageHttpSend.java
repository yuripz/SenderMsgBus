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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
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
                MessageSend_Log.info("[{}] sendSoapMessage.POST PropUser=`{}` PropPswd=`{}`", messageQueueVO.getQueue_Id(), PropUser, PropPswd);
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
                MessageSend_Log.info("[{}] sendSoapMessage.POST PropUser== null (`{}`)", messageQueueVO.getQueue_Id(), PropUser);
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
        messageQueueVO.setMsg_Date( Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());

        byte[] RequestBody;
        if (( messageDetails.MessageTemplate4Perform.getPropEncoding_Out() !=null ) &&
                ( !messageDetails.MessageTemplate4Perform.getPropEncoding_Out().equals("UTF-8" )) ) {
            try {
                RequestBody = SoapEnvelope.toString().getBytes( messageDetails.MessageTemplate4Perform.getPropEncoding_Out());
            } catch (UnsupportedEncodingException encodingExc) {
                System.err.println("[" + messageQueueVO.getQueue_Id() + "] sendSoapMessage.POST UnsupportedEncodingException");
                encodingExc.printStackTrace();
                MessageSend_Log.error("[{}] from {} to_UTF_8 fault:{}", messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getPropEncoding_Out(), encodingExc);
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
                MessageSend_Log.info("[{}]sendSoapMessage.POST({}).connectTimeoutInMillis={}.000;.readTimeoutInMillis={}.000;.PropUser={};.PropPswd={};.{}= {}",
                        messageQueueVO.getQueue_Id(), EndPointUrl, messageTemplate4Perform.getPropTimeout_Conn(),
                        messageTemplate4Perform.getPropTimeout_Read(), messageDetails.MessageTemplate4Perform.getPropUser(),
                        messageDetails.MessageTemplate4Perform.getPropPswd(), messageDetails.MessageTemplate4Perform.SOAP_ACTION_11,
                        messageDetails.MessageTemplate4Perform.getSOAPAction());
            if ( IsDebugged )
                MessageSend_Log.info("[{}]sendSoapMessage.POST[{}]", messageQueueVO.getQueue_Id(), SoapEnvelope);
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

            MessageSend_Log.warn("[{}]sendSoapMessage.POST.Response.getBody().length ={}", messageQueueVO.getQueue_Id(), Response.body().length);
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
                MessageSend_Log.error("[{}] IOUtils.toString from {} to_UTF_8 fault:{}", messageQueueVO.getQueue_Id(),
                        messageDetails.MessageTemplate4Perform.getPropEncoding_Out() == null ? "UTF_8" : messageDetails.MessageTemplate4Perform.getPropEncoding_Out(), ioExc);
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
                MessageSend_Log.info("[{}] HTTP status: {} sendSoapMessage.POST.Response=#(`{}`)", messageQueueVO.getQueue_Id(),
                                restResponseStatus, messageDetails.XML_MsgResponse.toString());
            if ( IsDebugged )
                theadDataAccess.doUPDATE_QUEUElog( ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse, MessageSend_Log );

        } catch ( Exception e) {
            // Журналируем ответ как есть
            MessageSend_Log.error("[{}]sendSoapMessage.POST ({}) fault:{}", messageQueueVO.getQueue_Id(), EndPointUrl, sStackTrace.strInterruptedException(e));
            messageDetails.MsgReason.append(" sendSoapMessage.POST (" ).append ( EndPointUrl )
                                    .append ( ") fault: " ).append ( sStackTrace.strInterruptedException(e));

            if ( IsDebugged )
                theadDataAccess.doUPDATE_QUEUElog( ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e), MessageSend_Log );

            // HE-4892 Если транспорт отвалился , то Шина ВСЁ РАВНО формирует как бы ответ , но с Fault внутри.
            // НАДО проверять количество порыток !!!
            MessageSend_Log.error("[{}]Retry_Count ({})>= ( ShortRetryCount={} LongRetryCount={})", messageQueueVO.getQueue_Id(),
                                            messageQueueVO.getRetry_Count(), messageDetails.MessageTemplate4Perform.getShortRetryCount(),
                                            messageDetails.MessageTemplate4Perform.getLongRetryCount());
            if ( messageQueueVO.getRetry_Count() +1  >= messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount() )
            {
                // количество порыток исчерпано, формируем результат для выхода из повторов
                MessageSend_Log.error("[{}]sendSoapMessage.POST ({}) fault:{}", messageQueueVO.getQueue_Id(), EndPointUrl, e);
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
        messageQueueVO.setMsg_Date( Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );

        try {
            // Получили ответ от сервиса, инициируем обработку SOAP getResponseBody()
            MessageSoapSend.getResponseBody (messageDetails, null, MessageSend_Log);
            if ( IsDebugged )
                MessageSend_Log.info("[{}]sendSoapMessage:ClearBodyResponse=({})", messageQueueVO.getQueue_Id(),
                                        messageDetails.XML_ClearBodyResponse.toString());
            else {// HE-9187
                if (messageDetails.XML_ClearBodyResponse.length() > 2049)
                    MessageSend_Log.info("[{}]sendSoapMessage:ClearBodyResponse[2048 char]=({}...)",
                                            messageQueueVO.getQueue_Id(), messageDetails.XML_ClearBodyResponse.substring(0, 2048));
                else
                    MessageSend_Log.info("[{}]sendSoapMessage:ClearBodyResponse[all char]=({}...)",
                                            messageQueueVO.getQueue_Id(), messageDetails.XML_ClearBodyResponse.toString());
                // client.wait(2048); --HE-10763 : Расширить размер логируемого сообщения ( ответ при ответ при сбое на стороне получателя ) до 2 кб
            }

        } catch (Exception e) {
            MessageSend_Log.error("[{}] Retry_Count={} SendSoapMessage.getResponseBody fault({} : {}", messageQueueVO.getQueue_Id(), messageQueueVO.getRetry_Count(), RestResponse, sStackTrace.strInterruptedException(e));
            MessageSend_Log.error("[{}] Sending SoapEnvelope[{}]", messageQueueVO.getQueue_Id(), SoapEnvelope);
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
            MessageSend_Log.error("[{}]sendSoapMessage.ApiRestHttpClient.close fault, Exception:{}", messageQueueVO.getQueue_Id(), IOE.getMessage());
        }
        ApiRestHttpClient = null;
        /*try {
            syncConnectionManager.close();
        } catch ( Exception anyE ) {
            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "HttpGetMessage.Unirest.syncConnectionManager.close fault, UnirestException:" + anyE);
        }
        syncConnectionManager = null;*/

    } finally {
        MessageSend_Log.warn("[{}]sendSoapMessage.ApiRestHttpClient.close finally", messageQueueVO.getQueue_Id());
        if (ApiRestHttpClient != null)
            try {
                ApiRestHttpClient.close();

            } catch ( Exception IOE ) {
                MessageSend_Log.error("[{}]sendSoapMessage.ApiRestHttpClient.close finally fault, Exception:{}", messageQueueVO.getQueue_Id(), IOE.getMessage());
            }
        ApiRestHttpClient = null; // for GC
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

    public static byte[] replaceUrlPlaceholders( String inputStr, boolean[]  isReplaceContent) {

        StringBuilder output = new StringBuilder();

        int currentIndex = 0; isReplaceContent[0] = false;

        while (true) {
            int startIdx = inputStr.indexOf( XMLchars.URL_File_Path_Begin, currentIndex);
            if (startIdx == -1) {
                // Нет больше маркеров, добавляем оставшуюся часть
                output.append(inputStr.substring(currentIndex));
                break;
            }
            // Добавляем часть перед маркером
            output.append(inputStr, currentIndex, startIdx);

            int pathStartIdx = startIdx + XMLchars.URL_File_Path_Begin.length();

            int endIdx = inputStr.indexOf(XMLchars.URL_File_Path_End, pathStartIdx);
            if (endIdx == -1) {
                // Нет закрывающего маркера, добавляем всё и завершаем
                output.append(inputStr.substring(startIdx));
                break;
            }

            // Извлекаем путь
            String filePath = inputStr.substring(pathStartIdx, endIdx);
            String fileContentBase64;
            try {
                byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
                // Конвертируем содержимое файла в base64 строку
                fileContentBase64 = Base64.getEncoder().encodeToString(fileBytes);
            } catch (IOException e) {
                // В случае ошибки, вставляем сообщение или оставляем маркер
                fileContentBase64 = "[Ошибка при чтении файла: " + filePath + "]";
            }

            // Вставляем содержимое файла
            output.append(fileContentBase64);
            isReplaceContent[0] = true;

            // Продвигаемся дальше
            currentIndex = endIdx + XMLchars.URL_File_Path_End.length();
        }

        return output.toString().getBytes(StandardCharsets.UTF_8);
    }

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
                MessageSend_Log.info("[{}] sendPostMessage.POST PropUser=`{}` PropPswd=`{}`", messageQueueVO.getQueue_Id(), PropUser, PropPswd);
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
                MessageSend_Log.info("[{}] sendPostMessage.POST PropUser== null (`{}`)", messageQueueVO.getQueue_Id(), PropUser);
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
        {  if (messageDetails.XML_MsgSEND.charAt(0) =='{')
            httpHeaders.put("Content-Type","application/json;charset=UTF-8");
            else {
            httpHeaders.put("Content-Type","application/x-www-form-urlencoded");
            //String urlEncoded = URLEncoder.encode(messageDetails.XML_MsgSEND, StandardCharsets.UTF_8);
            // messageDetails.XML_MsgSEND = urlEncoded;
        }
            if ( IsDebugged )
                MessageSend_Log.info("[{}] sendPostMessage.POST JSON `{}`", messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND);
        }
        else
        { httpHeaders.put("Content-Type", "text/xml;charset=UTF-8"); }

        if (( messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty) == -1 )// NOT Header_is_empty
           && ( ! messageDetails.Soap_HeaderRequest.isEmpty() ))
        {
            headerParams = messageDetails.Soap_HeaderRequest.toString().split(":");
            if ( IsDebugged ) {
                MessageSend_Log.info("[{}] sendPostMessage.POST headerParams.length={}", messageQueueVO.getQueue_Id(), headerParams.length);
                for (int i = 0; i < headerParams.length; i++)
                    MessageSend_Log.info("[{}] sendPostMessage.POST headerParams[{}] = {}", messageQueueVO.getQueue_Id(), i, headerParams[i]);
            }
            if (headerParams.length > 1  )
            for (int i = 0; i < headerParams.length; i++)
                httpHeaders.put(headerParams[0], headerParams[1]);
        }
        else { if ( IsDebugged )
            MessageSend_Log.info("[{}] sendPostMessage.POST indexOf(XMLchars.TagMsgHeaderEmpty)={}", messageQueueVO.getQueue_Id(),
                                        messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty));
        }
        // MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.Unirest.post `" + messageDetails.Soap_HeaderRequest + "` httpHeaders.size=" + httpHeaders.size() );
        //+                 "; headerParams= " + headerParams.toString() );
    try {
        boolean[] isReplaceContent4UrlPlaceholder = { false };

        if ((messageDetails.MessageTemplate4Perform.getPropEncoding_Out() != null) &&
                (!messageDetails.MessageTemplate4Perform.getPropEncoding_Out().equalsIgnoreCase("UTF-8"))) {
            try {
                RequestBody = messageDetails.XML_MsgSEND.getBytes(messageDetails.MessageTemplate4Perform.getPropEncoding_Out());
            } catch (UnsupportedEncodingException e) {
                System.err.println("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage: UnsupportedEncodingException");
                e.printStackTrace();
                MessageSend_Log.error("[{}] from {} to_UTF_8 fault:{}", messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getPropEncoding_Out(), sStackTrace.strInterruptedException(e));
                messageDetails.MsgReason.append(" sendPostMessage.post.to").append(messageDetails.MessageTemplate4Perform.getPropEncoding_Out()).append(" fault: ").append(sStackTrace.strInterruptedException(e));
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendPostMessage.POST", true, e, MessageSend_Log);
                return -1;
            }
        } else {
            if ( messageDetails.XML_MsgSEND.indexOf( XMLchars.URL_File_Path_Begin) > 0 )
                RequestBody = replaceUrlPlaceholders( messageDetails.XML_MsgSEND, isReplaceContent4UrlPlaceholder );
            else
                RequestBody = messageDetails.XML_MsgSEND.getBytes(StandardCharsets.UTF_8);
        }

        try {
            if ( IsDebugged )
                MessageSend_Log.info("[{}]sendPostMessage.POST({}).connectTimeoutInMillis={};.readTimeoutInMillis=ReadTimeoutInMillis= {} PropUser:{}",
                            messageQueueVO.getQueue_Id(), EndPointUrl, messageTemplate4Perform.getPropTimeout_Conn(), messageTemplate4Perform.getPropTimeout_Read(), PropUser);
            messageDetails.Confirmation.clear();
            messageDetails.XML_MsgResponse.setLength(0);

            if (IsDebugged) {
                if ( isReplaceContent4UrlPlaceholder[0] == false)
                    ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog(messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND, MessageSend_Log);
                else {
                    String QUEUElogString = new  String(RequestBody, StandardCharsets.UTF_8);
                    ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog(messageQueueVO.getQueue_Id(), QUEUElogString, MessageSend_Log);
                }
            }

            HttpRequest.Builder requestBuilder = java.net.http.HttpRequest.newBuilder();
            // добавляем все заголовки как есть через HttpRequest.Builder
            for (Map.Entry<String, String> entry: httpHeaders.entrySet()) {
                requestBuilder = requestBuilder
                                .header(entry.getKey(),entry.getValue());
                if ( IsDebugged )
                    MessageSend_Log.info("[{}] sendPostMessage.POST .header: `{}:{}`", messageQueueVO.getQueue_Id(), entry.getKey(), entry.getValue());
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

            MessageSend_Log.warn("[{}] sendPostMessage.Response httpCode={} getBody().length={}", messageQueueVO.getQueue_Id(), restResponseStatus, Response.body().length);
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
                MessageSend_Log.error("[{}] IOUtils.toString from `{}` to_UTF_8 fault:{}", messageQueueVO.getQueue_Id(), PropEncoding_Out, ioExc.getMessage());
                messageDetails.MsgReason.append(" HttpGetMessage.post.to_UTF_8 Encoding fault `")
                                                .append(PropEncoding_Out)
                                                .append("` :")
                                                .append(sStackTrace.strInterruptedException(ioExc));
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendPostMessage.POST", true, ioExc, MessageSend_Log);
                if (IsDebugged)
                    theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(ioExc), MessageSend_Log);
                return -1;
            }
            // обработку HTTP статусов 502, 503 и 504 от внешних систем как транспортную ошибку
            if (( restResponseStatus == 502)
                 || ( restResponseStatus == 503 )
                    || ( restResponseStatus == 504 )
               ) {
                Exception e = new Exception(" sendPostMessage.Response httpCode=" + Integer.toString(restResponseStatus  ) + "\n" + RestResponse );
                if (IsDebugged)
                    MessageSend_Log.error("[{}] sendPostMessage call handle_Transport_Errors: `{}`", messageQueueVO.getQueue_Id(), e.getMessage());
                return handle_Transport_Errors(theadDataAccess, messageQueueVO, messageDetails, EndPointUrl, "sendPostMessage.POST", e,
                        ROWID_QUEUElog, IsDebugged, MessageSend_Log);
            }


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
                        if ((RestResponse.startsWith("{") ) || (RestResponse.startsWith("[") ) ) { // Разбираем Json
                            try {
                                final String RestResponse_with_HttpResponseStatusCode = "{ \"HttpResponseStatusCode\":" + String.valueOf(restResponseStatus) + ",\"payload\":"
                                         + RestResponse + "}";
                                JSONObject RestResponseJSON = new JSONObject(RestResponse_with_HttpResponseStatusCode);
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
                MessageSend_Log.info("[{}] sendPostMessage.POST Envelope_MsgResponse=({})", messageQueueVO.getQueue_Id(), messageDetails.XML_MsgResponse.toString());


            // -- Задваивается в случае ошибки => это делается внутри ProcessingSendError()
            // messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);

        } catch (Exception e) {
            return handle_Transport_Errors ( theadDataAccess,  messageQueueVO,  messageDetails,  EndPointUrl,  "sendPostMessage.POST", e,
                     ROWID_QUEUElog,  IsDebugged,   MessageSend_Log);

            /*System.err.println("[" + messageQueueVO.getQueue_Id() + "]  Exception"); e.printStackTrace();

            MessageSend_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "sendPostMessage.POST (" + EndPointUrl + ") fault, HttpRequestException:" + e);
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
            return -1;*/
        }

        if (IsDebugged)
            MessageSend_Log.info("[{}] sendPostMessage.POST httpStatus=[{}], RestResponse=({})", messageQueueVO.getQueue_Id(), restResponseStatus, RestResponse);

        try {
            // Получили ответ от сервиса, инициируем обработку getResponseBody()
            InputStream parsedRestResponseStream;
            parsedRestResponseStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
            SAXBuilder documentBuilder = new SAXBuilder();
            Document XMLdocument;

            try {
                XMLdocument = documentBuilder.build(parsedRestResponseStream);
                if (IsDebugged)
                    MessageSend_Log.info("[{}] sendPostMessage documentBuilder=[{}], XML_MsgResponse=({})",
                                        messageQueueVO.getQueue_Id(), XMLdocument.toString(), messageDetails.XML_MsgResponse);

            } catch (JDOMException RestResponseE) {
                XMLdocument = null;
                MessageSend_Log.error("[{}]sendPostMessage.documentBuilder fault: {}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(RestResponseE));
                // формируем искуственный XML_MsgResponse из Fault ,  меняем XML_MsgResponse
                append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse );
            }

            MessageSoapSend.getResponseBody(messageDetails, XMLdocument, MessageSend_Log);

            if (IsDebugged)
                MessageSend_Log.info("[{}] sendPostMessage:ClearBodyResponse=({})", messageQueueVO.getQueue_Id(), messageDetails.XML_ClearBodyResponse.toString());
            // client.wait(100);

        } catch (Exception e) {
            System.err.println("[" + messageQueueVO.getQueue_Id() + "]  Exception");
            e.printStackTrace();
            MessageSend_Log.error("[{}] sendPostMessage.getResponseBody fault: {}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e));
            messageDetails.MsgReason.append(" sendPostMessage.getResponseBody fault: ").append(sStackTrace.strInterruptedException(e));

            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    "sendPostMessage.getResponseBody", true, e, MessageSend_Log);
            return -3;
        }
        if (restResponseStatus != 200) // Rest вызов считаем успешным только при получении
        {
            MessageSend_Log.error("[{}] sendPostMessage.restResponseStatus != 200: {}", messageQueueVO.getQueue_Id(), restResponseStatus);
            messageDetails.MsgReason.append(" sendPostMessage.restResponseStatus != 200: ").append(restResponseStatus);

            int messageRetry_Count = MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    "sendPostMessage.restResponseStatus != 200 ", false, null, MessageSend_Log);
            MessageSend_Log.error("[{}] sendPostMessage.messageRetry_Count = {}", messageQueueVO.getQueue_Id(), messageRetry_Count);
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
            MessageSend_Log.error("[{}] sendPostMessage.ApiRestHttpClient.close fault, Exception:{}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(IOE));
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
        MessageSend_Log.warn("[{}] sendPostMessage.ApiRestHttpClient.close finally", messageQueueVO.getQueue_Id());
        if (ApiRestHttpClient != null)
            try {
                ApiRestHttpClient.close();

            } catch ( Exception IOE ) {
                MessageSend_Log.error("[{}] sendPostMessage.ApiRestHttpClient.close finally fault, UnirestException:{}", messageQueueVO.getQueue_Id(), IOE.getMessage());
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

    private static int  handle_Transport_Errors ( TheadDataAccess theadDataAccess, MessageQueueVO messageQueueVO, MessageDetails messageDetails, String EndPointUrl, String colledHttpMethodName,
            Exception e,
                                 String ROWID_QUEUElog , boolean IsDebugged, Logger  MessageSend_Log)
    {
        System.err.println("[" + messageQueueVO.getQueue_Id() + "]  Exception"); if (e != null ) e.printStackTrace();

        MessageSend_Log.error("[{}] {} ({}) fault, HttpRequestException:{}", messageQueueVO.getQueue_Id(), colledHttpMethodName, EndPointUrl, sStackTrace.strInterruptedException(e));
        messageDetails.MsgReason.append(" sendPostMessage.POST fault: ");
        if (e != null ) messageDetails.MsgReason.append(sStackTrace.strInterruptedException(e));

        // Журналируем UnirestException-ответ как есть
        if (IsDebugged)
            theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e), MessageSend_Log);

        // HE-4892 Если транспорт отвалился , то Шина ВСЁ РАВНО формирует как бы ответ , но с Fault внутри.
        // НАДО проверять количество порыток !!!
        MessageSend_Log.error("[{}] {}: Retry_Count ({})>= ( ShortRetryCount={} LongRetryCount={})", messageQueueVO.getQueue_Id(),
                                            colledHttpMethodName, messageQueueVO.getRetry_Count(),
                                            messageDetails.MessageTemplate4Perform.getShortRetryCount(),
                                            messageDetails.MessageTemplate4Perform.getLongRetryCount());
        if (messageQueueVO.getRetry_Count() + 1 >= messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount()) {
            // количество порыток исчерпано, формируем результат для выхода из повторов
            MessageSend_Log.error("[{}] {}(`{}`) fault:{}", messageQueueVO.getQueue_Id(), colledHttpMethodName, EndPointUrl, e);
            append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, 506 ,
                    colledHttpMethodName + " (`").append(EndPointUrl).append("`) fault:").append(e.getMessage() );

            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    colledHttpMethodName + " (" + EndPointUrl + "), do re-Send: ", false, e, MessageSend_Log);
        } else {
            // HE-4892 Если транспорт отвалился , то Шина выставляет RESOUT - коммент ProcessingSendError & return -1;
            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    colledHttpMethodName + " (`" + EndPointUrl + "`) ", true, e, MessageSend_Log);
            return -1;
        }
        MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                colledHttpMethodName + " (`" + EndPointUrl + "`)", true, e, MessageSend_Log);
        return -1;
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

///////////////////////////////////// HttpDeleteMethod  ///////////////////////////////////////////////////////////////////////////////////////////////////

public static int HttpDeleteMessage(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess, Logger MessageSend_Log) {

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
                    "HttpDeleteMessage.setHttpGetParams() [" + messageDetails.MessageTemplate4Perform.getPropEncoding_Out() + " не поддерживается]", true, e, MessageSend_Log);
        }
        else
            MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                    "HttpDeleteMessage.setHttpGetParams() [не содержит параметров для HttpDelete]", true,  e ,  MessageSend_Log);
        return -1;
    }
    if ( numOfParams < 1) {
        MessageUtils.ProcessingSendError(  messageQueueVO,   messageDetails,  theadDataAccess,
                "HttpDeleteMessage.setHttpGetParams(): [вызов не содержит параметров для HttpDelete]", true,  null ,  MessageSend_Log);
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
            MessageSend_Log.info("[{}] HttpDeleteMessage.DELETE PropUser=`{}` PropPswd=`{}`", messageQueueVO.getQueue_Id(), PropUser, PropPswd);
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
            MessageSend_Log.info("[{}] HttpDeleteMessage.DELETE PropUser== null (`{}`)", messageQueueVO.getQueue_Id(), PropUser);
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
            MessageSend_Log.info("[{}] HttpDeleteMessage.DELETE({}).connectTimeoutInMillis={};.readTimeoutInMillis=ReadTimeoutInMillis= {}; User={}; Pswd{}; numOfParams={}",
                                messageQueueVO.getQueue_Id(), EndPointUrl, messageTemplate4Perform.getPropTimeout_Conn(),
                                messageTemplate4Perform.getPropTimeout_Read(), messageDetails.MessageTemplate4Perform.getPropUser(),
                                messageDetails.MessageTemplate4Perform.getPropPswd(), numOfParams);
            messageDetails.Confirmation.clear();
            messageDetails.XML_MsgResponse.setLength(0);

            StringBuilder queryString= new StringBuilder(messageDetails.XML_MsgSEND.length());

            // Для обращений к внешней системе НЕЛЬЗЯ передавать автоматом Queue_Id !
            // первый проход -- ище параметр, который пойдёт как добавка URL типа /Query_KEY_Value
            for (Map.Entry<String, String> entry: HttpGetParams.entrySet()) {
                if (entry.getKey().equalsIgnoreCase("Query_KEY_Value")) {
                    queryString.append("/").append(entry.getValue());
                }

            }
            // queryString.append("?queue_id=").append(messageQueueVO.getQueue_Id());
            int j=0;
            for (Map.Entry<String, String> entry: HttpGetParams.entrySet()) {
                if (! entry.getKey().equalsIgnoreCase("Query_KEY_Value"))
                { // Query_KEY_Value в Qry не добавляем!
                    if (j == 0) {
                        {
                            queryString.append("?");
                            j++;
                        }
                    } else {
                        queryString.append("&");
                    }
                    // добавляем параметр в Qry
                    queryString.append(entry.getKey()).append("=").append(entry.getValue());
                }

            }

            Escaper restElmntEscaper = UrlEscapers.urlFragmentEscaper();
            //restElmntEscaper.escape(queryString.toString());
            URI URI_4_GET = URI.create(EndPointUrl + restElmntEscaper.escape(queryString.toString()));

            if (IsDebugged) {
                MessageSend_Log.info("[{}] HttpDeleteMessage.DELETE URI=`{}{}`", messageQueueVO.getQueue_Id(), EndPointUrl, restElmntEscaper.escape(queryString.toString()));
                ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog(messageQueueVO.getQueue_Id(), queryString.toString(), MessageSend_Log);
            }

            // формируем заголовки с учетом переменных httpHeaders из параметров
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

            if (( messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty) == -1 )// NOT Header_is_empty
                    && ( ! messageDetails.Soap_HeaderRequest.isEmpty() ))
            {
                headerParams = messageDetails.Soap_HeaderRequest.toString().split(":");
                if ( IsDebugged ) {
                    MessageSend_Log.info("[{}] HttpDeleteMessage.DELETE headerParams.length={}", messageQueueVO.getQueue_Id(), headerParams.length);
                    for (int i = 0; i < headerParams.length; i++)
                        MessageSend_Log.info("[{}] HttpDeleteMessage.DELETE headerParams[{}] = {}", messageQueueVO.getQueue_Id(), i, headerParams[i]);
                }
                if (headerParams.length > 1  )
                    for (int i = 0; i < headerParams.length; i++)
                        httpHeaders.put(headerParams[0], headerParams[1]);
            }
            else { if ( IsDebugged )
                MessageSend_Log.info("[{}] HttpDeleteMessage.DELETE indexOf(XMLchars.TagMsgHeaderEmpty)={}", messageQueueVO.getQueue_Id(), messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty));
            }

            HttpRequest.Builder requestBuilder = java.net.http.HttpRequest.newBuilder();
            // добавляем все заголовки как есть через HttpRequest.Builder
            for (Map.Entry<String, String> entry: httpHeaders.entrySet()) {
                requestBuilder = requestBuilder
                        .header(entry.getKey(),entry.getValue());
                if ( IsDebugged )
                    MessageSend_Log.info("[{}] HttpDeleteMessage.DELETE .header: `{}:{}`", messageQueueVO.getQueue_Id(), entry.getKey(), entry.getValue());
                // queryString.append(entry.getKey()).append("=").append(entry.getValue());
            }

            java.net.http.HttpRequest request = requestBuilder
                    .DELETE()
                    .uri( URI_4_GET)
                    //.header("User-Agent", "msgBus/Java-21") -- добавили уже выще, удалить после отладки
                    //.header("Accept", "*/*")
                    //.header("Connection", "close")
                    .timeout( Duration.ofSeconds( messageTemplate4Perform.getPropTimeout_Read()) )
                    .build();
            RestResponseGet = ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofString() );
            RestResponse = RestResponseGet.body(); //.toString();
            // messageDetails.SimpleHttpClient.
            restResponseStatus = RestResponseGet.statusCode();

            // -- Задваивается в случае ошибки => это делается внутри ProcessingSendError()
            // messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);
            if (IsDebugged)
                MessageSend_Log.info("[{}] HttpDeleteMessage.DELETE RestResponse=({})", messageQueueVO.getQueue_Id(), RestResponse);
            // MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Unirest.get escapeXml.RestResponse=(" + XML.escape(RestResponse) + ")");

        } catch (Exception e) {
            return handle_Transport_Errors ( theadDataAccess,  messageQueueVO,  messageDetails,  EndPointUrl,  "HttpDeleteMessage.DELETE", e,
                    ROWID_QUEUElog,  IsDebugged,   MessageSend_Log);
        }

        if ((messageDetails.MessageTemplate4Perform.getPropEncoding_Out() != null) &&
                (!messageDetails.MessageTemplate4Perform.getPropEncoding_Out().equals("UTF-8"))) {
            try {
                RestResponse = XML.to_UTF_8(RestResponse, messageDetails.MessageTemplate4Perform.getPropEncoding_Out());
            } catch (Exception e) {
                System.err.println("[" + messageQueueVO.getQueue_Id() + "] UnsupportedEncodingException");
                e.printStackTrace();
                MessageSend_Log.error("[{}] from {} to_UTF_8 fault:{}", messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getPropEncoding_Out(), sStackTrace.strInterruptedException(e));
                messageDetails.MsgReason.append(" HttpDeleteMessage.DELETE.to_UTF_8 fault: ").append(sStackTrace.strInterruptedException(e));
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "HttpDeleteMessage.DELETE", true, e, MessageSend_Log);
                if (IsDebugged)
                    theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e), MessageSend_Log);
                return -1;
            }
        }
        // обработку HTTP статусов 502, 503 и 504 от внешних систем как транспортную ошибку
        if (( restResponseStatus == 502)
                || ( restResponseStatus == 503 )
                || ( restResponseStatus == 504 )
        ) {
            Exception e = new Exception(" sendHttpDeleteMessage.DELETE.Response httpCode=" + Integer.toString(restResponseStatus  ) + "\n" + RestResponse );
            return handle_Transport_Errors(theadDataAccess, messageQueueVO, messageDetails, EndPointUrl, "HttpDeleteMessage.DELETE", e,
                    ROWID_QUEUElog, IsDebugged, MessageSend_Log);
        }
        if (IsDebugged)
            theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse, MessageSend_Log);

        try {
            Document XMLdocument;
            try {
                // если в ответ пришла пустая строка, то заменяем её на пустой JSON "[]"
                if (RestResponse.isEmpty()) RestResponse="[]";
                final String RestResponse_with_HttpResponseStatusCode = "{ \"HttpResponseStatusCode\":" + String.valueOf(restResponseStatus) + ",\"payload\":"
                        + RestResponse + "}";
                JSONObject RestResponseJSON = new JSONObject(RestResponse_with_HttpResponseStatusCode);
                // JSONObject RestResponseJSON = new JSONObject(RestResponse);
                messageDetails.XML_MsgResponse.append(XMLchars.Envelope_Begin);
                messageDetails.XML_MsgResponse.append(XMLchars.Body_Begin);
                XML.setMessege_Log(MessageSend_Log);
                messageDetails.XML_MsgResponse.append(XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse));
                messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
                messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);
                MessageSend_Log.info("[{}] HttpDeleteMessage.DELETE: RestResponse(`{}`) - xmlResponse=({})", messageQueueVO.getQueue_Id(), RestResponse_with_HttpResponseStatusCode, messageDetails.XML_MsgResponse.toString());

                ByteArrayInputStream parsedRestResponseStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
                SAXBuilder documentBuilder = new SAXBuilder();

                XMLdocument = documentBuilder.build(parsedRestResponseStream);
            } catch (JDOMException RestResponseE) {

                System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpDeleteMessage.JSONObject Exception" + RestResponseE.getMessage());
                RestResponseE.printStackTrace();
                System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpDeleteMessage.RestResponse[" + RestResponse + "]");
                MessageSend_Log.error("[{}] HttpDeleteMessage.getResponseBody fault: {}" , messageQueueVO.getQueue_Id(),  sStackTrace.strInterruptedException(RestResponseE));
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
            MessageSend_Log.info("[{}] HttpDeleteMessage.DELETE :ClearBodyResponse `{}`" , messageQueueVO.getQueue_Id(),  messageDetails.XML_ClearBodyResponse.toString() );
            // client.wait(100);

        } catch (Exception e) {
            System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpDeleteMessage.JSONObject Exception");
            e.printStackTrace();
            System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpDeleteMessage.RestResponse[" + RestResponse + "]");
            MessageSend_Log.error("[{}] HttpDeleteMessage.getResponseBody fault: {}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e));
            messageDetails.MsgReason.append(" HttpDeleteMessage.getResponseBody fault: ").append(sStackTrace.strInterruptedException(e));

            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    "HttpDeleteMessage.getResponseBody", true, e, MessageSend_Log);
            return -3;
        }
    } // делаем, всё, что можно и нужно
    catch ( Exception allE) {
        if (ApiRestHttpClient != null)
            try {
                ApiRestHttpClient.close();

            } catch ( Exception IOE ) {
                MessageSend_Log.error("[{}] HttpDeleteMessage.ApiRestHttpClient.close fault, Exception:{}", messageQueueVO.getQueue_Id(), IOE.getMessage());
            }
        ApiRestHttpClient = null;

    } finally {
        MessageSend_Log.warn("[{}] HttpDeleteMessage.ApiRestHttpClient.close finally", messageQueueVO.getQueue_Id());
        if (ApiRestHttpClient != null)
            try {
                ApiRestHttpClient.close();

            } catch ( Exception IOE ) {
                MessageSend_Log.error("[{}] HttpDeleteMessage.ApiRestHttpClient.close finally fault, Exception:{}", messageQueueVO.getQueue_Id(), IOE.getMessage());
            }
        ApiRestHttpClient = null;

    }
    return 0;
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
                    "HttpGetMessage.setHttpGetParams(): [вызов не содержит параметров для HtthGet]", true,  null ,  MessageSend_Log);
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
                MessageSend_Log.info("[{}] HttpGetMessage.GET PropUser=`{}` PropPswd=`{}`", messageQueueVO.getQueue_Id(), PropUser, PropPswd);
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
                MessageSend_Log.info("[{}] HttpGetMessage.GET PropUser== null (`{}`)", messageQueueVO.getQueue_Id(), PropUser);
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
          MessageSend_Log.info("[{}] HttpGetMessage.GET({}).connectTimeoutInMillis={};.readTimeoutInMillis=ReadTimeoutInMillis= {}; User={}; Pswd{}; numOfParams={}",
                                                messageQueueVO.getQueue_Id(), EndPointUrl, messageTemplate4Perform.getPropTimeout_Conn(),
                                                messageTemplate4Perform.getPropTimeout_Read(), messageDetails.MessageTemplate4Perform.getPropUser(),
                                                messageDetails.MessageTemplate4Perform.getPropPswd(), numOfParams);
          messageDetails.Confirmation.clear();
          messageDetails.XML_MsgResponse.setLength(0);

          StringBuilder queryString= new StringBuilder(messageDetails.XML_MsgSEND.length());

          // Для обращений к внешней системе НЕЛЬЗЯ передавать автоматом Queue_Id !
          // первый проход -- ище параметр, который пойдёт как добавка URL типа /Query_KEY_Value
          for (Map.Entry<String, String> entry: HttpGetParams.entrySet()) {
                  if (entry.getKey().equalsIgnoreCase("Query_KEY_Value")) {
                      queryString.append("/").append(entry.getValue());
                  }

          }
          // queryString.append("?queue_id=").append(messageQueueVO.getQueue_Id());
          int j=0;
          for (Map.Entry<String, String> entry: HttpGetParams.entrySet()) {
              if (! entry.getKey().equalsIgnoreCase("Query_KEY_Value"))
              { // Query_KEY_Value в Qry не добавляем!
                  if (j == 0) {
                      {
                          queryString.append("?");
                          j++;
                      }
                  } else {
                      queryString.append("&");
                  }
                    // добавляем параметр в Qry
                  queryString.append(entry.getKey()).append("=").append(entry.getValue());
              }

          }

          Escaper restElmntEscaper = UrlEscapers.urlFragmentEscaper();
          //restElmntEscaper.escape(queryString.toString());
          URI URI_4_GET = URI.create(EndPointUrl + restElmntEscaper.escape(queryString.toString()));

          if (IsDebugged) {
              MessageSend_Log.info("[{}] HttpGetMessage.GET URI=`{}{}`", messageQueueVO.getQueue_Id(), EndPointUrl, restElmntEscaper.escape(queryString.toString()));
              ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog(messageQueueVO.getQueue_Id(), queryString.toString(), MessageSend_Log);
          }

          // формируем заголовки с учетом переменных httpHeaders из параметров
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

          if (( messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty) == -1 )// NOT Header_is_empty
                  && ( ! messageDetails.Soap_HeaderRequest.isEmpty() ))
          {
              headerParams = messageDetails.Soap_HeaderRequest.toString().split(":");
              if ( IsDebugged ) {
                  MessageSend_Log.info("[{}] HttpGetMessage.GET headerParams.length={}", messageQueueVO.getQueue_Id(), headerParams.length);
                  for (int i = 0; i < headerParams.length; i++)
                      MessageSend_Log.info("[{}] HttpGetMessage.GET headerParams[{}] = {}", messageQueueVO.getQueue_Id(), i, headerParams[i]);
              }
              if (headerParams.length > 1  )
                  for (int i = 0; i < headerParams.length; i++)
                      httpHeaders.put(headerParams[0], headerParams[1]);
          }
          else { if ( IsDebugged )
              MessageSend_Log.info("[{}] HttpGetMessage.GET indexOf(XMLchars.TagMsgHeaderEmpty)={}", messageQueueVO.getQueue_Id(), messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty));
          }

          HttpRequest.Builder requestBuilder = java.net.http.HttpRequest.newBuilder();
          // добавляем все заголовки как есть через HttpRequest.Builder
          for (Map.Entry<String, String> entry: httpHeaders.entrySet()) {
              requestBuilder = requestBuilder
                      .header(entry.getKey(),entry.getValue());
              if ( IsDebugged )
                  MessageSend_Log.info("[{}] HttpGetMessage.GET .header: `{}:{}`", messageQueueVO.getQueue_Id(), entry.getKey(), entry.getValue());
              // queryString.append(entry.getKey()).append("=").append(entry.getValue());
          }

          java.net.http.HttpRequest request = requestBuilder
                  .GET( )
                  .uri( URI_4_GET)
                  //.header("User-Agent", "msgBus/Java-21") -- добавили уже выще, удалить после отладки
                  //.header("Accept", "*/*")
                  //.header("Connection", "close")
                  .timeout( Duration.ofSeconds( messageTemplate4Perform.getPropTimeout_Read()) )
                  .build();
          RestResponseGet = ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofString() );
          RestResponse = RestResponseGet.body(); //.toString();
          // messageDetails.SimpleHttpClient.
          restResponseStatus = RestResponseGet.statusCode();

          // -- Задваивается в случае ошибки => это делается внутри ProcessingSendError()
          // messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);
          if (IsDebugged)
              MessageSend_Log.info("[{}] HttpGetMessage.GET RestResponse=({})", messageQueueVO.getQueue_Id(), RestResponse);
          // MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Unirest.get escapeXml.RestResponse=(" + XML.escape(RestResponse) + ")");

      } catch (Exception e) {
          return handle_Transport_Errors ( theadDataAccess,  messageQueueVO,  messageDetails,  EndPointUrl,  "HttpGetMessage.GET", e,
                  ROWID_QUEUElog,  IsDebugged,   MessageSend_Log);
      }

      if ((messageDetails.MessageTemplate4Perform.getPropEncoding_Out() != null) &&
              (!messageDetails.MessageTemplate4Perform.getPropEncoding_Out().equals("UTF-8"))) {
          try {
              RestResponse = XML.to_UTF_8(RestResponse, messageDetails.MessageTemplate4Perform.getPropEncoding_Out());
          } catch (Exception e) {
              System.err.println("[" + messageQueueVO.getQueue_Id() + "] UnsupportedEncodingException");
              e.printStackTrace();
              MessageSend_Log.error("[{}] from {} to_UTF_8 fault:{}", messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getPropEncoding_Out(), e.toString());
              messageDetails.MsgReason.append(" HttpGetMessage.get.to_UTF_8 fault: ").append(sStackTrace.strInterruptedException(e));
              MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                      "HttpGetMessage.GET", true, e, MessageSend_Log);
              if (IsDebugged)
                  theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e), MessageSend_Log);
              return -1;
          }
      }
      // обработку HTTP статусов 502, 503 и 504 от внешних систем как транспортную ошибку
      if (( restResponseStatus == 502)
              || ( restResponseStatus == 503 )
              || ( restResponseStatus == 504 )
      ) {
          Exception e = new Exception(" sendGetMessage.Response httpCode=" + Integer.toString(restResponseStatus  ) + "\n" + RestResponse );
          return handle_Transport_Errors(theadDataAccess, messageQueueVO, messageDetails, EndPointUrl, "HttpGetMessage.GET", e,
                  ROWID_QUEUElog, IsDebugged, MessageSend_Log);
      }
      if (IsDebugged)
          theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse, MessageSend_Log);

      try {
          Document XMLdocument;
          try {
              final String RestResponse_with_HttpResponseStatusCode = "{ \"HttpResponseStatusCode\":" + String.valueOf(restResponseStatus) + ",\"payload\":"
                      + RestResponse + "}";
              JSONObject RestResponseJSON = new JSONObject(RestResponse_with_HttpResponseStatusCode);
              // JSONObject RestResponseJSON = new JSONObject(RestResponse);
              messageDetails.XML_MsgResponse.append(XMLchars.Envelope_Begin);
              messageDetails.XML_MsgResponse.append(XMLchars.Body_Begin);
              XML.setMessege_Log(MessageSend_Log);
              messageDetails.XML_MsgResponse.append(XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse));
              messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
              messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);
              MessageSend_Log.info("[{}] HttpGetMessage.GET: Response=({})", messageQueueVO.getQueue_Id(), messageDetails.XML_MsgResponse.toString());

              ByteArrayInputStream parsedRestResponseStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
              SAXBuilder documentBuilder = new SAXBuilder();

              XMLdocument = documentBuilder.build(parsedRestResponseStream);
          } catch (JDOMException RestResponseE) {

              System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpGetMessage.JSONObject Exception" + RestResponseE.getMessage());
              RestResponseE.printStackTrace();
              System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpGetMessage.RestResponse[" + RestResponse + "]");
              MessageSend_Log.error("[{}] HttpGetMessage.getResponseBody fault: {}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(RestResponseE));
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
          MessageSend_Log.info("[{}] HttpGetMessage.GET :ClearBodyResponse=({})", messageQueueVO.getQueue_Id(), messageDetails.XML_ClearBodyResponse.toString());
          // client.wait(100);

      } catch (Exception e) {
          System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpGetMessage.JSONObject Exception");
          e.printStackTrace();
          System.err.println("[" + messageQueueVO.getQueue_Id() + "] HttpGetMessage.RestResponse[" + RestResponse + "]");
          MessageSend_Log.error("[{}] HttpGetMessage.getResponseBody fault: {}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e));
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
          MessageSend_Log.error("[{}] HttpGetMessage.ApiRestHttpClient.close fault, Exception:{}", messageQueueVO.getQueue_Id(), IOE.getMessage());
      }
      ApiRestHttpClient = null;

      } finally {
      MessageSend_Log.warn("[{}] HttpGetMessage.ApiRestHttpClient.close finally", messageQueueVO.getQueue_Id());
          if (ApiRestHttpClient != null)
              try {
                  ApiRestHttpClient.close();

              } catch ( Exception IOE ) {
                  MessageSend_Log.error("[{}] HttpGetMessage.ApiRestHttpClient.close finally fault, Exception:{}", messageQueueVO.getQueue_Id(), IOE.getMessage());
              }
          ApiRestHttpClient = null;
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
                    MessageSend_Log.info("[{}] setHttpGetParams=({}=>`{}`", Queue_Id, RestElmnt.getName(), paramsInXml.toString());
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
                    MessageSend_Log.info("[{}] WebRestExePostExec PropUser=`{}` PropPswd=`{}`", messageQueueVO.getQueue_Id(), PropUser, PropPswd);
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
                    MessageSend_Log.info("[{}] WebRestExePostExec.4.GET PropUser== null, PreemptivePostExec (`{}`)", messageQueueVO.getQueue_Id(), messageTemplate4Perform.getPreemptivePostExec());
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
                     MessageSend_Log.info("[{}] Authorization Basic {} (using User=`{}` Pswd=`{}`)", messageQueueVO.getQueue_Id(), encodedAuth, PropUser, PropPswd);
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
                MessageSend_Log.info("[{}] WebRestExePostExec.GET({}?queue_id={}) httpStatus=[{}] RestResponse=(`{}`)",
                                     messageQueueVO.getQueue_Id(), EndPointUrl, String.valueOf(Queue_Id), restResponseStatus, RestResponse);

        } catch ( Exception e) {
            // возмущаемся, но оставляем сообщение в ResOUT что бы обработчик в кроне мог доработать
            MessageSend_Log.error("[{}] Ошибка пост-обработки HttpGet({}?queue_id={}), вызов от имени пользователя(`{}/{}`):{}",
                                    messageQueueVO.getQueue_Id(), EndPointUrl, String.valueOf(Queue_Id), PropUser, PropPswd, e.toString());
            theadDataAccess.doUPDATE_MessageQueue_SetMsg_Reason(messageQueueVO,
                    "Ошибка пост-обработки HttpGet(" + EndPointUrl + "), вызов от имени пользователя(`"+ PropUser +"`):" + sStackTrace.strInterruptedException(e), 123567,
                    messageQueueVO.getRetry_Count(),  MessageSend_Log);
             if ( ApiRestHttpClient !=null) ApiRestHttpClient.close();
            return -17L;
        }
        try {
            JSONObject RestResponseJSON = new JSONObject( RestResponse );
            MessageSend_Log.info("[{}] WebRestExePostExec=(`{}`)", messageQueueVO.getQueue_Id(), RestResponseJSON.toString());
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

            MessageSend_Log.warn("[{}] Пост-обработчик HttpGet не вернул JSon({}):{}", messageQueueVO.getQueue_Id(), EndPointUrl, e.toString());
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
                    MessageSend_Log.info("[{}] WebRestExePostExec.GET PropUser=`{}` PropPswd=`{}`", messageQueueVO.getQueue_Id(), PropUser, PropPswd);
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
                    MessageSend_Log.info("[{}] WebRestExePostExec.GET PropUser== null (`{}`)", messageQueueVO.getQueue_Id(), PropUser);
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
                MessageSend_Log.info("[{}] WebRestErrOUTPostExec.get({}) httpStatus=[{}] RestResponse=(`{}`)", messageQueueVO.getQueue_Id(), EndPointUrl, restResponseStatus, RestResponse);

        } catch ( Exception e) {
            // возмущаемся, но оставляем сообщение в ResOUT что бы обработчик в кроне мог доработать
            MessageSend_Log.error("[{}] Ошибка пост-обработки WebRestErrOUTPostExec HttpGet({}):{}", messageQueueVO.getQueue_Id(), EndPointUrl, e.toString());
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
