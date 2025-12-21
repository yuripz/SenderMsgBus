package net.plumbing.msgbus.threads.utils;

import com.google.common.escape.Escaper;
import com.jayway.jsonpath.*;

import net.plumbing.msgbus.common.json.JSONException;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.model.*;
import net.plumbing.msgbus.threads.TheadDataAccess;
import net.sf.saxon.s9api.*;
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

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpHeaders;
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
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.XPathExpressionException;
import java.nio.charset.StandardCharsets;
//import java.sql.RowId;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
//import java.util.Arrays;
import java.util.*;
//import java.util.concurrent.TimeUnit;
// import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;

import static net.plumbing.msgbus.common.XMLchars.OpenTag;
import static net.plumbing.msgbus.threads.utils.MessageUtils.stripNonValidXMLCharacters;

// Record для хранения результата
record ElementInfo(String elementName, XdmNode element, String formDataFieldName, String contentType, String fileName, String isJsonMadeManually) { }

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

    public static byte[] replaceUrlPlaceholders( String inputStr, boolean[]  isReplaceContent, long Queue_Id, int ApiRestWaitTime,
                                                 MessageTemplate4Perform messageTemplate4Perform,
                                                 Logger MessageSend_Log ) throws IOException, SaxonApiException {


        int startIdx = inputStr.indexOf( XMLchars.URL_File_Path_Begin, 0);
        if (startIdx == -1) {
            // Нет маркеров, выходим как есть
            return inputStr.getBytes(StandardCharsets.UTF_8);
        }
        StringBuilder output = new StringBuilder();

        int currentIndex = 0; isReplaceContent[0] = false;

        while (true) {
            startIdx = inputStr.indexOf( XMLchars.URL_File_Path_Begin, currentIndex);
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
            String httpURLFilePath = inputStr.substring(pathStartIdx, endIdx);
            String fileContentBase64;
            try {
                byte[] fileBytes = readBytesFromUrlWithAuthPreemptive(httpURLFilePath, ApiRestWaitTime, Queue_Id,
                                                        messageTemplate4Perform, MessageSend_Log);
                // Конвертируем содержимое файла в base64 строку
                fileContentBase64 = Base64.getEncoder().encodeToString(fileBytes);
            } catch (IOException IOe) {
                MessageSend_Log.error("[{}][Ошибка при чтении URL:`{}`: {} ", Queue_Id, httpURLFilePath, IOe.getMessage());
                // В случае ошибки, вставляем сообщение или оставляем маркер
                fileContentBase64 = "`[]`" + Queue_Id +"[Ошибка при чтении URL: " + httpURLFilePath + "]";
                // В случае ошибки, генерим IOException
                throw new IOException ("Ошибка при чтении URL:`"+ httpURLFilePath +"`: "+ IOe.getMessage() + " , полученного для передачи вложений", IOe);
            }

            // Вставляем содержимое файла
            output.append(fileContentBase64);
            isReplaceContent[0] = true;

            // Продвигаемся дальше
            currentIndex = endIdx + XMLchars.URL_File_Path_End.length();
        }

        return output.toString().getBytes(StandardCharsets.UTF_8);
    }

    // Парсим XML строку в XdmNode
    public static XdmNode parseXml(String xmlString, Processor xPathProcessor )  {
        try {
            DocumentBuilder xPathBuilder = xPathProcessor.newDocumentBuilder();
            StreamSource xmlSource = new StreamSource(new StringReader(xmlString));
            return xPathBuilder.build(xmlSource);

        } catch (SaxonApiException e) {
            throw new RuntimeException("Ошибка парсинга XML `" + xmlString +"` :" + e.getMessage(), e);
            //throw new SaxonApiException("Ошибка парсинга XML `" + xmlString +"` :" + e.getMessage(), e);
        }
    }

    // Получаем все элементы, у которых есть атрибуты formDataFieldName и ContentType
    public static List<ElementInfo> extractElements(@NotNull XdmNode XMLdoc, @NotNull XPathSelector xPathSelector) {
        List<ElementInfo> results = new ArrayList<>();
        try {
            // XPath: выбрать все элементы, у которых есть атрибуты formDataFieldName и ContentType
            //XPathSelector selector = xpathCompiler.compile("//*[@formDataFieldName and @ContentType]").load();
            xPathSelector.setContextItem(XMLdoc);

            for (XdmItem item : xPathSelector) {
                if (item instanceof XdmNode node && node.getNodeKind() == XdmNodeKind.ELEMENT) {
                    QName qName = node.getNodeName();
                    String formDataFieldName = node.getAttributeValue(new QName("formDataFieldName"));
                    String contentType = node.getAttributeValue(new QName("ContentType"));
                    String fileName  = node.getAttributeValue(new QName("filename"));
                    String isJsonMadeManually = node.getAttributeValue(new QName("isJsonMade"));
                    results.add(new ElementInfo(qName.getLocalName(), node, formDataFieldName, contentType, fileName, isJsonMadeManually));
                }
            }
        } catch (SaxonApiException e) {
            throw new RuntimeException("Ошибка выполнения XPath для получения значений атрибутов", e);
        }
        return results;
    }
    // Обьединение: парсим и ищем
    public static List<ElementInfo> processXml( @NotNull Processor xPathProcessor,@NotNull XPathSelector xPathSelector, String xmlString) {
        XdmNode XMLdoc = parseXml(xmlString, xPathProcessor );
        return extractElements(XMLdoc, xPathSelector);
    }

    public static byte[] readBytesFromUrlWithAuthPreemptive(@NotNull String EndPointUrl, int ApiRestWaitTime, long Queue_Id,
                                                            MessageTemplate4Perform messageTemplate4Perform,
                                                            Logger MessageSend_Log  ) throws IOException {
        URL url = new URL(EndPointUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        String PropUser = messageTemplate4Perform.getPropUserPostExec();
        String PropPswd = messageTemplate4Perform.getPropPswdPostExec();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(ApiRestWaitTime *1000);

        // Создаем строку для Basic Auth
        String auth = PropUser + ":" + PropPswd;
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        if ( messageTemplate4Perform.getIsDebugged() )
            MessageSend_Log.info("[{}] Authorization Basic {} (using User=`{}` Pswd=`{}`)",Queue_Id, encodedAuth, PropUser, PropPswd);
        connection.setRequestProperty("Authorization", "Basic " + encodedAuth);

        try (InputStream in = connection.getInputStream();
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int n;
            while ((n = in.read(buffer)) != -1) {
                baos.write(buffer, 0, n);
            }
            return baos.toByteArray();
        }
    }

    public static byte[] getFileContentByURL(@NotNull String  EndPointUrl, MessageQueueVO messageQueueVO, MessageTemplate4Perform messageTemplate4Perform,
                                           int ApiRestWaitTime, Logger MessageSend_Log ) {
        byte[] RestResponse = null;
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
                    .uri( URI.create(EndPointUrl ))
                    .header("User-Agent", "msgBus/Java-21")
                    .header("Accept", "*/*")
                    .header("Connection", "close")
                    .timeout( Duration.ofSeconds( ApiRestWaitTime ) )
                    .build();
            HttpResponse<byte[]> RestResponseGet = ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofByteArray() );
            RestResponse = RestResponseGet.body(); //.toString();

            restResponseStatus = RestResponseGet.statusCode(); //500; //RestResponseGet.statusCode();

            if ( messageTemplate4Perform.getIsDebugged() )
                MessageSend_Log.info("[{}] WebRestExePostExec.GET({}?queue_id={}) httpStatus=[{}] RestResponse=(`{}`)",
                        messageQueueVO.getQueue_Id(), EndPointUrl, String.valueOf(Queue_Id), restResponseStatus, RestResponse);

        } catch ( Exception e) {
            // возмущаемся, но оставляем сообщение в ResOUT что бы обработчик в кроне мог доработать
            MessageSend_Log.error("[{}] Ошибка получения файла HttpGet `{}`, вызов от имени пользователя(`{}/{}`):{}",
                    messageQueueVO.getQueue_Id(), EndPointUrl,  PropUser, PropPswd, e.toString());
            ApiRestHttpClient.close();
            return null;
        }

        ApiRestHttpClient.close();
        return RestResponse;

    }

    public static int sendWebFormMessage(@NotNull String saved_XML_MsgSEND, @NotNull Processor xPathProcessor,@NotNull XPathSelector xPathSelector,
            @NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess, int ApiRestWaitTime, Logger MessageSend_Log) {
        //
        MessageTemplate4Perform messageTemplate4Perform = messageDetails.MessageTemplate4Perform;

        String EndPointUrl;
        String ROWID_QUEUElog=null;
        if ( StringUtils.substring(messageTemplate4Perform.getEndPointUrl(),0,"http".length()).equalsIgnoreCase("http") )
            EndPointUrl = messageTemplate4Perform.getEndPointUrl();
        else
            EndPointUrl = "http://" + messageTemplate4Perform.getEndPointUrl();

        String formDataFieldName = ""; // messageQueueVO.getMsg_Type_own(); // по-умолчанию используем собственный тип
        List<ElementInfo> elements = processXml(xPathProcessor, xPathSelector,
                                                                saved_XML_MsgSEND);
        boolean IsDebugged = messageDetails.MessageTemplate4Perform.getIsDebugged();
        for (ElementInfo info : elements) {
            if (IsDebugged ) {
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMessage.Элемент: {}", messageQueueVO.getQueue_Id(), info.elementName());
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMessage.Метка элемента: {}", messageQueueVO.getQueue_Id(), info.element().toString());
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMessage.formDataFieldName: {} ", messageQueueVO.getQueue_Id(), info.formDataFieldName());
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMessage.ContentType: {}", messageQueueVO.getQueue_Id(), info.contentType());
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMessage.filename: {}", messageQueueVO.getQueue_Id(), info.fileName());
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMessage.isJsonMadeManually: {}", messageQueueVO.getQueue_Id(), info.isJsonMadeManually());
            }
            if ( info.elementName().equalsIgnoreCase("Query_KEY_Value"))
            {
                EndPointUrl = EndPointUrl + info.element().getStringValue();
            }
            if ((info.formDataFieldName() != null) && ( !info.formDataFieldName().equalsIgnoreCase("null")) )
            {
                formDataFieldName = info.formDataFieldName();
            }
        }
        // добавляем получение хвоста из /HelpMeCancelTicket_Request/Query_KEY_Value
        // String Query_KEY_Value = getURL_from_Query_KEY_Value(saved_XML_MsgSEND);


        // int ConnectTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Conn() * 1000;
        // int ReadTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Read() * 1000;
        String RestResponse;
        int restResponseStatus;
        String AckXSLT_4_make_JSON = messageTemplate4Perform.getAckXSLT() ;


        HttpClient ApiRestHttpClient;

        String PropUser = messageDetails.MessageTemplate4Perform.getPropUser();
        String PropPswd = messageDetails.MessageTemplate4Perform.getPropPswd();
        if ( (messageDetails.MessageTemplate4Perform.restPasswordAuthenticator != null) &&
                (!messageDetails.MessageTemplate4Perform.getIsPreemptive())  // adding the header to the HttpRequest and removing Authenticator
        )
        {
            if ( IsDebugged ) {
                MessageSend_Log.info("[{}] sendWebFormMessage.POST PropUser=`{}` PropPswd=`{}`", messageQueueVO.getQueue_Id(), PropUser, PropPswd);
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
                MessageSend_Log.info("[{}] sendWebFormMessage.POST PropUser== null (`{}`)", messageQueueVO.getQueue_Id(), PropUser);
            ApiRestHttpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .connectTimeout(Duration.ofSeconds(messageTemplate4Perform.getPropTimeout_Conn()))
                    .build();
        }
        StringBuilder RequestBody = new StringBuilder(messageDetails.XML_MsgSEND.length() + 128 );

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
        String boundary = "----------------" + UUID.randomUUID().toString().replace("-", "");
        httpHeaders.put("Content-Type","multipart/form-data; boundary=" + boundary);

        if  (formDataFieldName.isEmpty() ||
            (! ((AckXSLT_4_make_JSON == null) || AckXSLT_4_make_JSON.isEmpty() ) ) // с помощью AckXSLT_4_make_JSON было выполнено преобразование в
                                                                                  // JSON должно быть проверено
            ) // messageQueueVO.getMsg_Type_own(); // по-умолчанию используем собственный тип
        {
            // если XML с помощью .MessageXSLT. не метили тегами formDataFieldName, то преобразование к JSON должно быть в .AckXSLT.
            if ((messageDetails.XML_MsgSEND.charAt(0) == '{') || (messageDetails.XML_MsgSEND.charAt(0) == '[')) {
                if (IsDebugged)
                    MessageSend_Log.info("[{}] sendWebFormMessage.POST JSON `{}`", messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND);
                try { // проверяем, получили ли валидный JSON после AckXSLT
                    JSONObject SendedJSONvalue = new JSONObject(messageDetails.XML_MsgSEND); // new JSONObject(kafkaRecord.value());

                    messageDetails.XML_MsgSEND = SendedJSONvalue.toString(0);
                    if (IsDebugged)
                        MessageSend_Log.info("[{}] sendWebFormMessageJSONObject: `{}`", messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND);

                } catch (JSONException jsonEx) {
                    MessageSend_Log.error("[{}] sendWebFormMessageJSONObject Ошибка при получении объекта как JSONObject, проверьте AckXSLT! - (`{}` , ...) return: {}",
                            messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND, jsonEx.getMessage());
                    return handle_Transport_Errors(theadDataAccess, messageQueueVO, messageDetails, EndPointUrl,
                            "sendWebFormMessage.POST, Ошибка при получении объекта как JSONObject", jsonEx,
                            ROWID_QUEUElog, IsDebugged, MessageSend_Log);
                }
            } else {
                MessageSend_Log.error("[{}] sendWebFormMessage.POST NOT JSON (XML с помощью .MessageXSLT. не метили тегами formDataFieldName, то преобразование к JSON должно быть в .AckXSLT) `{}`",
                        messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND);
                // TO_DO: здесь надо бы формировать ошибку с руганью
                if (IsDebugged)
                    ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog(messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND, MessageSend_Log);

                Exception e = new Exception("[" + messageQueueVO.getQueue_Id() + "] sendWebFormMessage.POST NOT JSON! ```\n" + messageDetails.XML_MsgSEND + "\n'```");
                MessageSend_Log.error("[{}] sendWebFormMessage: XML с помощью .MessageXSLT. не метили тегами formDataFieldName: `{}`", messageQueueVO.getQueue_Id(), e.getMessage());
                return handle_Transport_Errors(theadDataAccess, messageQueueVO, messageDetails, EndPointUrl, "sendWebFormMessage.POST", e,
                        ROWID_QUEUElog, IsDebugged, MessageSend_Log);
            }
        } // проверяем на JSon если одиночная форма и не метили тегами formDataFieldName, то преобразование к JSON должно быть в .AckXSLT.

        if (( messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty) == -1 )// NOT Header_is_empty
                && ( ! messageDetails.Soap_HeaderRequest.isEmpty() ))
        {
            headerParams = messageDetails.Soap_HeaderRequest.toString().split(":");
            if ( IsDebugged ) {
                MessageSend_Log.info("[{}] sendWebFormMessage.POST headerParams.length={}", messageQueueVO.getQueue_Id(), headerParams.length);
                for (int i = 0; i < headerParams.length; i++)
                    MessageSend_Log.info("[{}] sendWebFormMessage.POST headerParams[{}] = {}", messageQueueVO.getQueue_Id(), i, headerParams[i]);
            }
            if (headerParams.length > 1  )
                for (int i = 0; i < headerParams.length; i++)
                    httpHeaders.put(headerParams[0], headerParams[1]);
        }
        else { if ( IsDebugged )
            MessageSend_Log.info("[{}] sendWebFormMessage.POST indexOf(XMLchars.TagMsgHeaderEmpty)={}", messageQueueVO.getQueue_Id(),
                    messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty));
        }
        // MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.Unirest.post `" + messageDetails.Soap_HeaderRequest + "` httpHeaders.size=" + httpHeaders.size() );
        //+                 "; headerParams= " + headerParams.toString() );
        try {

            boolean[] isReplaceContent4UrlPlaceholder = { false };

            // Используется только UTF-8 кодировка, прочие игнорируем
            if  (formDataFieldName.isEmpty()  ) //
                {     // messageQueueVO.getMsg_Type_own(); // по-умолчанию используем собственный тип
                    MessageSend_Log.warn("[{}] RequestBody sendWebFormMessage.formDataFieldName: {} для {}, BO={} " , messageQueueVO.getQueue_Id() ,
                                        messageQueueVO.getMsg_Type_own(), messageQueueVO.getMsg_Type(), messageQueueVO.getOperation_Id());
                        RequestBody.append("--").append(boundary).append("\r\n")
                                .append("Content-Disposition: form-data; name=\"")
                                .append( messageQueueVO.getMsg_Type_own() )
                                .append("\"\r\n")
                                .append("Content-Type: application/json\r\n\r\n")
                                ;
                        if ( messageDetails.XML_MsgSEND.indexOf( XMLchars.URL_File_Path_Begin) > 0 )
                            RequestBody.append(IOUtils.toString(replaceUrlPlaceholders(messageDetails.XML_MsgSEND, isReplaceContent4UrlPlaceholder,
                                                messageQueueVO.getQueue_Id(), ApiRestWaitTime, messageTemplate4Perform, MessageSend_Log),
                                    "UTF-8" ));
                        else
                            RequestBody.append(messageDetails.XML_MsgSEND);

                            RequestBody.append("\r\n")
                                .append("--").append(boundary)
                                .append("--").append("\r\n")
                        ;
                }
            else { // для каждой секции с formDataFieldName формируем multipart
                        for (ElementInfo info : elements) {
                            if ((info.formDataFieldName() != null) && ( !info.formDataFieldName().equalsIgnoreCase("null")) )
                            {
                                MessageSend_Log.warn("[{}] RequestBody sendWebFormMessage.Элемент: {}", messageQueueVO.getQueue_Id(), info.elementName() );
                                MessageSend_Log.warn("[{}] RequestBody sendWebFormMessage.formDataFieldName: {} " , messageQueueVO.getQueue_Id() , info.formDataFieldName());
                                MessageSend_Log.warn("[{}] RequestBody sendWebFormMessage.ContentType: {}", messageQueueVO.getQueue_Id() , info.contentType());
                                MessageSend_Log.warn("[{}] RequestBody sendWebFormMessage.isJsonMadeManually: {}", messageQueueVO.getQueue_Id() , info.isJsonMadeManually());
                                formDataFieldName = info.formDataFieldName();
                                RequestBody.append("--").append(boundary).append("\r\n")
                                        .append("Content-Disposition: form-data; name=\"")
                                        .append( info.formDataFieldName() );
                                if (info.fileName() != null) {
                                    RequestBody.append("; filename=\"").append( info.fileName() );
                                }
                                RequestBody.append("\"\r\n")
                                        .append("Content-Type: ").append(info.contentType()).append("\r\n\r\n")
                                        ;
                                if ( info.contentType().contains("json") ) {
                                    if ((info.isJsonMadeManually() !=null) && ( info.isJsonMadeManually().contains("true") )) {
                                        MessageSend_Log.warn("[{}] RequestBody sendWebFormMessage.Тело элемента: {}", messageQueueVO.getQueue_Id(),
                                                                            messageDetails.XML_MsgSEND);
                                        RequestBody.append( messageDetails.XML_MsgSEND)
                                                   .append("\r\n");

                                    }
                                    else {
                                        MessageSend_Log.warn("[{}] RequestBody sendWebFormMessage.Метка элемента: {}", messageQueueVO.getQueue_Id(), info.element().toString());
                                        XdmNode parentNode = info.element();
                                        // Предполагаем, что внутри один дочерний элемент
                                        XdmNode childNode = null;
                                        for (XdmSequenceIterator<XdmNode> it = parentNode.axisIterator(Axis.CHILD); ; ) {
                                            XdmNode child = it.next();
                                            childNode = child;
                                            break; // берем только первый
                                        }
                                        if (childNode != null) {
                                            RequestBody.append(
                                                            XML.toJSONObject(childNode.toString()).toString(0)
                                                    )
                                                    .append("\r\n")
                                            ;
                                        } else
                                            RequestBody.append("[]")
                                                    .append("\r\n")
                                                    ;
                                    }
                                } else {
                                    if (info.fileName() == null) {// text/plain
                                        MessageSend_Log.warn("[{}] RequestBody sendWebFormMessage.содержимое элемента: {}", messageQueueVO.getQueue_Id(), info.element().getStringValue());
                                        if (info.element().getStringValue().indexOf(XMLchars.URL_File_Path_Begin) > 0)
                                            RequestBody.append(IOUtils.toString(replaceUrlPlaceholders(info.element().getStringValue(), isReplaceContent4UrlPlaceholder,
                                                            messageQueueVO.getQueue_Id(), ApiRestWaitTime, messageTemplate4Perform, MessageSend_Log), "UTF-8"))
                                                    .append("\r\n");
                                        else
                                            RequestBody.append(info.element().getStringValue())
                                                    .append("\r\n")
                                                    ;
                                    }
                                    else {
                                        MessageSend_Log.warn("[{}] RequestBody sendWebFormMessage.путь к файлу: {}", messageQueueVO.getQueue_Id(),
                                                info.element().getStringValue());
                                        try {
                                        RequestBody.append(IOUtils.toString(replaceUrlPlaceholders(info.element().getStringValue(), isReplaceContent4UrlPlaceholder,
                                                        messageQueueVO.getQueue_Id(), ApiRestWaitTime, messageTemplate4Perform, MessageSend_Log), "UTF-8"))
                                                  .append("\r\n");
                                        } catch (Exception  sendIoExc) {
                                            MessageSend_Log.error("[{}] sendWebFormMessage.replaceUrlPlaceholders fault={}", messageQueueVO.getQueue_Id(),
                                                                     sStackTrace.strInterruptedException (sendIoExc));

                                            // Missing www-authenticate header when receiving 401 responses.
            /*
            As per section 4.1 of RFC-7235, when an HTTP server returns a 401 response, it must also return a WWW-Authenticate header :
            A server generating a 401 (Unauthorized) response MUST send a
            WWW-Authenticate header field containing at least one challenge.
            However, when the refinitiv server returns 401, it returns the following header :
            Authorization: WWW-Authenticate: Signature realm="World-Check One API",algorithm="hmac-sha256",headers="(request-target) host date content-type content-length"
             */
                                            System.err.println("[" + messageQueueVO.getQueue_Id() + "] sendWebFormMessage.POST ApiRestHttpClient.send IOException: `" + sendIoExc.getMessage() + "`");
                                            sendIoExc.printStackTrace();
                                            return handle_Transport_Errors ( theadDataAccess,  messageQueueVO,  messageDetails,  EndPointUrl,  "sendWebFormMessage.POST", sendIoExc,
                                                    ROWID_QUEUElog,  IsDebugged,   MessageSend_Log);
                                        }
                                        // записываем байты
                                        // в конце (после каждого файла) добавим \r\n
                                        // Для этого лучше собрать байтовый массив, но для простоты используем StringBuilder с encode
                                        // или создаем массив байтов вручную позже

                                    }
                                }

                            }
                        }
                        RequestBody.append("--").append(boundary).append("--").append("\r\n");
            }
                    // = ( formDataFieldName + "=" + URLEncoder.encode( messageDetails.XML_MsgSEND, StandardCharsets.UTF_8) );

            try {
                if ( IsDebugged ) {
                    MessageSend_Log.info("[{}] sendWebFormMessage.formDataFieldName '{}' as `{}` to (`{}`)",
                            messageQueueVO.getQueue_Id(), formDataFieldName, RequestBody, EndPointUrl);
                    MessageSend_Log.info("[{}] sendWebFormMessage UTL to (`{}`).connectTimeoutInMillis={};.readTimeoutInMillis=ReadTimeoutInMillis= {} PropUser:{}",
                            messageQueueVO.getQueue_Id(),  EndPointUrl, messageTemplate4Perform.getPropTimeout_Conn(), messageTemplate4Perform.getPropTimeout_Read(), PropUser);
                }
                messageDetails.Confirmation.clear();
                messageDetails.XML_MsgResponse.setLength(0);

                if (IsDebugged) {
                        ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog(messageQueueVO.getQueue_Id(), RequestBody.toString(), MessageSend_Log);

                }

                HttpRequest.Builder requestBuilder = java.net.http.HttpRequest.newBuilder();
                // добавляем все заголовки как есть через HttpRequest.Builder
                for (Map.Entry<String, String> entry: httpHeaders.entrySet()) {
                    requestBuilder = requestBuilder
                            .header(entry.getKey(),entry.getValue());
                    if ( IsDebugged )
                        MessageSend_Log.info("[{}] sendWebFormMessage.POST .header: `{}:{}`", messageQueueVO.getQueue_Id(), entry.getKey(), entry.getValue());
                    // queryString.append(entry.getKey()).append("=").append(entry.getValue());
                }
                java.net.http.HttpRequest request = requestBuilder
                        .POST( HttpRequest.BodyPublishers.ofString(RequestBody.toString()) )
                        .uri(URI.create(EndPointUrl))
                        .timeout( Duration.ofSeconds( messageTemplate4Perform.getPropTimeout_Read()) )
                        .build();
                HttpResponse<String> Response= null;
                try {
                    Response= ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofString() );

                } catch (IOException  sendIoExc) {
                    MessageSend_Log.error("[{}] sendWebFormMessage.ApiRestHttpClient (.isTerminated=`{}`).send fault={}", messageQueueVO.getQueue_Id(),
                            ApiRestHttpClient.isTerminated(),
                            sStackTrace.strInterruptedException (sendIoExc));

                    // Missing www-authenticate header when receiving 401 responses.
            /*
            As per section 4.1 of RFC-7235, when an HTTP server returns a 401 response, it must also return a WWW-Authenticate header :
            A server generating a 401 (Unauthorized) response MUST send a
            WWW-Authenticate header field containing at least one challenge.
            However, when the refinitiv server returns 401, it returns the following header :
            Authorization: WWW-Authenticate: Signature realm="World-Check One API",algorithm="hmac-sha256",headers="(request-target) host date content-type content-length"
             */
                    System.err.println("[" + messageQueueVO.getQueue_Id() + "] sendWebFormMessage.POST ApiRestHttpClient.send IOException: `" + sendIoExc.getMessage() + "`");
                    sendIoExc.printStackTrace();
                    return handle_Transport_Errors ( theadDataAccess,  messageQueueVO,  messageDetails,  EndPointUrl,  "sendWebFormMessage.POST", sendIoExc,
                            ROWID_QUEUElog,  IsDebugged,   MessageSend_Log);
                }

                restResponseStatus = Response.statusCode();

                //Test = Response.getBody();
                // HttpHeaders responseHttpHeaders = null;
                HttpHeaders responseHttpHeaders = Response.headers();
                //MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Response getHeaders()=" + headers.all().toString() +" getHeaders().size=" + headers.size() );

                MessageSend_Log.warn("[{}] sendWebFormMessage.Response httpCode={} getBody().length={}", messageQueueVO.getQueue_Id(), restResponseStatus, Response.body().length());
                // MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Response getBody()=" + Arrays.toString(Test) +" getBody().length=" + Test.length );

                if ( restResponseStatus == 200)
                {
                String[] payloadResponsePartLines = Response.body().split("\n", 2);
                    if (payloadResponsePartLines.length < 1)
                    {
                        System.err.println("Неверный формат ответа");

                            Exception e = new Exception(" sendWebFormMessage.Response lines.length=" + Integer.toString( payloadResponsePartLines.length  ) + "\n" + Response.body() );
                            if (IsDebugged)
                                MessageSend_Log.error("[{}] sendWebFormMessage call handle_Transport_Errors: `{}`", messageQueueVO.getQueue_Id(), e.getMessage());
                            return handle_Transport_Errors(theadDataAccess, messageQueueVO, messageDetails, EndPointUrl, "sendWebFormMessage.POST", e,
                                    ROWID_QUEUElog, IsDebugged, MessageSend_Log);
                    }
                    // String firstLine = lines[0]; // например, "33"
                    // String payloadResponsePart = payloadResponsePartLines[1];
                    // вторая строка содержит полезную нагрузку
                    RestResponse = stripNonValidXMLCharacters( payloadResponsePartLines[0] ); // StandardCharsets.UTF_8);
                }
                else {
                    MessageSend_Log.warn("[{}] sendWebFormMessage.Response httpCode !=200 ={}, getBody()=`{}` responseHttpHeaders{}",
                            messageQueueVO.getQueue_Id(), restResponseStatus, Response.body(), responseHttpHeaders.toString() );
                    if (Response.body().isEmpty())
                    RestResponse = "[]";
                    else
                        RestResponse = Response.body();
                }

                if (IsDebugged)
                        theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse, MessageSend_Log);


                // обработку HTTP статусов 502, 503 и 504 от внешних систем как транспортную ошибку
                if (( restResponseStatus == 502)
                        || ( restResponseStatus == 503 )
                        || ( restResponseStatus == 504 )
                ) {
                    Exception e = new Exception(" sendWebFormMessage.Response httpCode=" + Integer.toString(restResponseStatus  ) + "\n" + RestResponse );
                    if (IsDebugged)
                        MessageSend_Log.error("[{}] sendWebFormMessage call handle_Transport_Errors: `{}`", messageQueueVO.getQueue_Id(), e.getMessage());
                      return handle_Transport_Errors(theadDataAccess, messageQueueVO, messageDetails, EndPointUrl, "sendWebFormMessage.POST", e,
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
                    append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , null, responseHttpHeaders );
                } else // получили НЕпустой ответ, пробуем его разобрать
                {
                     // должны получить  Json
                            if ((RestResponse.startsWith("{") ) || (RestResponse.startsWith("[") ) )
                            { // Разбираем Json
                                try {
                                    //final String
                                    StringBuilder
                                            RestResponse_with_HttpResponseStatusCode = new StringBuilder("{ \"HttpResponseStatusCode\":" + String.valueOf(restResponseStatus) + ",");
                                    RestResponse_with_HttpResponseStatusCode.append("\"HeadersHTTP\": {");
                                    do_Append_responseHttpHeaders_2_jSon( RestResponse_with_HttpResponseStatusCode, responseHttpHeaders );
                                    RestResponse_with_HttpResponseStatusCode.append(" },");
                                    RestResponse_with_HttpResponseStatusCode.append("\"payload\":")
                                            .append(RestResponse)
                                            .append("}");
                                    if (IsDebugged)
                                        MessageSend_Log.info("[{}] sendWebFormMessage.POST RestResponseJSON=({})", messageQueueVO.getQueue_Id(), RestResponse_with_HttpResponseStatusCode.toString());

                                    JSONObject RestResponseJSON = new JSONObject( RestResponse_with_HttpResponseStatusCode.toString() );
                                    String XML_MsgResponse_Body = XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse);
                                    if (IsDebugged)
                                        MessageSend_Log.info("[{}] sendWebFormMessage.POST XML_MsgResponse_Body=({})", messageQueueVO.getQueue_Id(), XML_MsgResponse_Body);

                                    messageDetails.XML_MsgResponse.append( XML_MsgResponse_Body );
                                    messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
                                    messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);
                                    if (IsDebugged)
                                        theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse_with_HttpResponseStatusCode.toString(), MessageSend_Log);
                                } catch (Exception JSONe) { // получили непонятно что
                                    MessageSend_Log.error("[{}] sendWebFormMessage.POST Exception JSONe получили непонятно что=({})", messageQueueVO.getQueue_Id(),
                                            JSONe.getMessage());

                                    // Кладем полученный ответ в <MsgData><![CDATA[" RestResponse "]]></MsgData>
                                    append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse, responseHttpHeaders );
                                }

                            } else {
                                MessageSend_Log.error("[{}] sendWebFormMessage.POST UNKNOWN ответ и не `{` и не `<` - опять же получили непонятно что=({})", messageQueueVO.getQueue_Id(),
                                        RestResponse);
                                // ответ и не `{` и не `<` - опять же получили непонятно что
                                // Кладем полученный ответ в <MsgData><![CDATA[" RestResponse "]]></MsgData>
                                append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse, responseHttpHeaders );
                            }
                }

                if (IsDebugged)
                    MessageSend_Log.info("[{}] sendWebFormMessage.POST Envelope_MsgResponse=({})", messageQueueVO.getQueue_Id(), messageDetails.XML_MsgResponse.toString());


                // -- Задваивается в случае ошибки => это делается внутри ProcessingSendError()
                // messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);

            } catch (Exception e) {
                return handle_Transport_Errors ( theadDataAccess,  messageQueueVO,  messageDetails,  EndPointUrl,  "sendPostMessage.POST", e,
                        ROWID_QUEUElog,  IsDebugged,   MessageSend_Log);

            }

            if (IsDebugged)
                MessageSend_Log.info("[{}] sendWebFormMessage.POST httpStatus=[{}], RestResponse=({})", messageQueueVO.getQueue_Id(), restResponseStatus, RestResponse);

            try {
                // Получили ответ от сервиса, инициируем обработку getResponseBody()
                InputStream parsedRestResponseStream;
                parsedRestResponseStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
                SAXBuilder documentBuilder = new SAXBuilder();
                Document XMLdocument;

                try {
                    XMLdocument = documentBuilder.build(parsedRestResponseStream);
                    if (IsDebugged)
                        MessageSend_Log.info("[{}] sendWebFormMessage documentBuilder info=`{}`, XML_MsgResponse=({})",
                                messageQueueVO.getQueue_Id(), XMLdocument.toString(), messageDetails.XML_MsgResponse);

                } catch (JDOMException RestResponseE) {
                    XMLdocument = null;
                    MessageSend_Log.error("[{}]sendWebFormMessage.documentBuilder fault: {}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(RestResponseE));
                    // формируем искуственный XML_MsgResponse из Fault ,  меняем XML_MsgResponse
                    append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse, null );
                }

                MessageSoapSend.getResponseBody(messageDetails, XMLdocument, MessageSend_Log);

                if (IsDebugged)
                    MessageSend_Log.info("[{}] sendWebFormMessage:ClearBodyResponse=({})", messageQueueVO.getQueue_Id(), messageDetails.XML_ClearBodyResponse.toString());
                // client.wait(100);

            } catch (Exception e) {
                System.err.println("[" + messageQueueVO.getQueue_Id() + "]  Exception");
                e.printStackTrace();
                MessageSend_Log.error("[{}] sendWebFormMessage.getResponseBody fault: {}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e));
                messageDetails.MsgReason.append(" sendWebFormMessage.getResponseBody fault: ").append(sStackTrace.strInterruptedException(e));

                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendWebFormMessage.getResponseBody", true, e, MessageSend_Log);
                return -3;
            }
            if (restResponseStatus != 200) // Rest вызов считаем успешным только при получении
            {
                MessageSend_Log.error("[{}] sendWebFormMessage.restResponseStatus != 200: {}", messageQueueVO.getQueue_Id(), restResponseStatus);
                messageDetails.MsgReason.append(" sendWebFormMessage.restResponseStatus != 200: ").append(restResponseStatus);

                int messageRetry_Count = MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendWebFormMessage.restResponseStatus != 200 ", false, null, MessageSend_Log);
                MessageSend_Log.error("[{}] sendWebFormMessage.messageRetry_Count = {}", messageQueueVO.getQueue_Id(), messageRetry_Count);
                if ( messageDetails.XML_ClearBodyResponse.length() > XMLchars.nanXSLT_Result.length() )
                    return 0; // ответ от внешней системы разобран в виде XML, надо продолжить обработку
                else
                    return -5; // и restResponseStatus != 200 и ответ неразбрчив
            } else
                return 0;
        } catch ( Exception allE) {
            if (ApiRestHttpClient != null)
                try {
                    ApiRestHttpClient.close();

                } catch ( Exception IOE ) {
                    MessageSend_Log.error("[{}] sendWebFormMessage.ApiRestHttpClient.close fault, Exception:{}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(IOE));
                }
            ApiRestHttpClient = null;

        } finally {
            MessageSend_Log.warn("[{}] sendWebFormMessage.ApiRestHttpClient.close finally", messageQueueVO.getQueue_Id());
            if (ApiRestHttpClient != null)
                try {
                    ApiRestHttpClient.close();

                } catch ( Exception IOE ) {
                    MessageSend_Log.error("[{}] sendWebFormMessage.ApiRestHttpClient.close finally fault, UnirestException:{}", messageQueueVO.getQueue_Id(), IOE.getMessage());
                }
            ApiRestHttpClient = null;
        }
        return 0;
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
                MessageSend_Log.error("[{}] sendPostMessage.POST from {} to_UTF_8 fault:{}", messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getPropEncoding_Out(), sStackTrace.strInterruptedException(e));
                messageDetails.MsgReason.append(" sendPostMessage.post.to").append(messageDetails.MessageTemplate4Perform.getPropEncoding_Out()).append(" fault: ").append(sStackTrace.strInterruptedException(e));
                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendPostMessage.POST", true, e, MessageSend_Log);
                return -1;
            }
        } else {
            if ( messageDetails.XML_MsgSEND.indexOf( XMLchars.URL_File_Path_Begin) > 0 )
                RequestBody = replaceUrlPlaceholders( messageDetails.XML_MsgSEND, isReplaceContent4UrlPlaceholder,
                        messageQueueVO.getQueue_Id(), 120, messageTemplate4Perform, MessageSend_Log);
            else
                RequestBody = messageDetails.XML_MsgSEND.getBytes(StandardCharsets.UTF_8);
        }

        try {
            if ( IsDebugged )
                MessageSend_Log.info("[{}] sendPostMessage.POST({}).connectTimeoutInMillis={};.readTimeoutInMillis=ReadTimeoutInMillis= {} PropUser:{}",
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
            HttpResponse<byte[]> Response= null;
        try {
               Response= ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofByteArray() );

                } catch (IOException  sendIoExc) {
            MessageSend_Log.error("[{}] sendPostMessage.ApiRestHttpClient (.isTerminated=`{}`).send fault={}", messageQueueVO.getQueue_Id(),
                    ApiRestHttpClient.isTerminated(),
                    sStackTrace.strInterruptedException (sendIoExc));

            // Missing www-authenticate header when receiving 401 responses.
            /*
            As per section 4.1 of RFC-7235, when an HTTP server returns a 401 response, it must also return a WWW-Authenticate header :
            A server generating a 401 (Unauthorized) response MUST send a
            WWW-Authenticate header field containing at least one challenge.
            However, when the refinitiv server returns 401, it returns the following header :
            Authorization: WWW-Authenticate: Signature realm="World-Check One API",algorithm="hmac-sha256",headers="(request-target) host date content-type content-length"
             */
            System.err.println("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.POST: ApiRestHttpClient.send.IOException: `" + sendIoExc.getMessage() + "`");
            sendIoExc.printStackTrace();
            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    "sendPostMessage.POST", true, sendIoExc, MessageSend_Log);
            if (IsDebugged)
                theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(sendIoExc), MessageSend_Log);
            return -1;
                }

            restResponseStatus = Response.statusCode();

            //Test = Response.getBody();
            // HttpHeaders responseHttpHeaders = null;
            HttpHeaders responseHttpHeaders = Response.headers();
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
                MessageSend_Log.error("[{}] sendPostMessage.POST: IOUtils.toString from `{}` to_UTF_8 fault:{}", messageQueueVO.getQueue_Id(), PropEncoding_Out, ioExc.getMessage());
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
                    MessageSend_Log.error("[{}] sendPostMessage.POST call handle_Transport_Errors: `{}`", messageQueueVO.getQueue_Id(), e.getMessage());
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
                append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , null, responseHttpHeaders );
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
                                //final String
                                StringBuilder
                                        RestResponse_with_HttpResponseStatusCode = new StringBuilder("{ \"HttpResponseStatusCode\":" + String.valueOf(restResponseStatus) + ",");
                                        RestResponse_with_HttpResponseStatusCode.append("\"HeadersHTTP\": {");
                                do_Append_responseHttpHeaders_2_jSon( RestResponse_with_HttpResponseStatusCode, responseHttpHeaders );
                                        RestResponse_with_HttpResponseStatusCode.append(" },");
                                        RestResponse_with_HttpResponseStatusCode.append("\"payload\":")
                                        .append(RestResponse)
                                        .append("}");
                                if (IsDebugged)
                                    MessageSend_Log.info("[{}] sendPostMessage.POST RestResponseJSON=({})", messageQueueVO.getQueue_Id(), RestResponse_with_HttpResponseStatusCode.toString());

                                JSONObject RestResponseJSON = new JSONObject( RestResponse_with_HttpResponseStatusCode.toString() );
                                String XML_MsgResponse_Body = XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse);
                                if (IsDebugged)
                                    MessageSend_Log.info("[{}] sendPostMessage.POST XML_MsgResponse_Body=({})", messageQueueVO.getQueue_Id(), XML_MsgResponse_Body);

                                messageDetails.XML_MsgResponse.append( XML_MsgResponse_Body );
                                messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
                                messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);
                                if (IsDebugged)
                                    theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse_with_HttpResponseStatusCode.toString(), MessageSend_Log);
                            } catch (Exception JSONe) { // получили непонятно что
                                    MessageSend_Log.error("[{}] sendPostMessage.POST Exception JSONe получили непонятно что=({})", messageQueueVO.getQueue_Id(),
                                            JSONe.getMessage());

                                // Кладем полученный ответ в <MsgData><![CDATA[" RestResponse "]]></MsgData>
                                append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse, responseHttpHeaders );
                                }

                        } else {
                                MessageSend_Log.error("[{}] sendPostMessage.POST UNKNOWN ответ и не `{` и не `<` - опять же получили непонятно что=({})", messageQueueVO.getQueue_Id(),
                                        RestResponse);
                            // ответ и не `{` и не `<` - опять же получили непонятно что
                            // Кладем полученный ответ в <MsgData><![CDATA[" RestResponse "]]></MsgData>
                            append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse, responseHttpHeaders );
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
                    MessageSend_Log.info("[{}] sendPostMessage.POST documentBuilder info=`{}`, XML_MsgResponse=({})",
                                        messageQueueVO.getQueue_Id(), XMLdocument.toString(), messageDetails.XML_MsgResponse);

            } catch (JDOMException RestResponseE) {
                XMLdocument = null;
                MessageSend_Log.error("[{}]sendPostMessage.POST.documentBuilder fault: {}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(RestResponseE));
                // формируем искуственный XML_MsgResponse из Fault ,  меняем XML_MsgResponse
                append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse, null );
            }

            MessageSoapSend.getResponseBody(messageDetails, XMLdocument, MessageSend_Log);

            if (IsDebugged)
                MessageSend_Log.info("[{}] sendPostMessage.POST:ClearBodyResponse=({})", messageQueueVO.getQueue_Id(), messageDetails.XML_ClearBodyResponse.toString());
            // client.wait(100);

        } catch (Exception e) {
            System.err.println("[" + messageQueueVO.getQueue_Id() + "]  Exception");
            e.printStackTrace();
            MessageSend_Log.error("[{}] sendPostMessage.POST.getResponseBody fault: {}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e));
            messageDetails.MsgReason.append(" sendPostMessage.getResponseBody fault: ").append(sStackTrace.strInterruptedException(e));

            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    "sendPostMessage.getResponseBody", true, e, MessageSend_Log);
            return -3;
        }
        if (restResponseStatus != 200) // Rest вызов считаем успешным только при получении
        {
            MessageSend_Log.error("[{}] sendPostMessage.POST.restResponseStatus != 200: {}", messageQueueVO.getQueue_Id(), restResponseStatus);
            messageDetails.MsgReason.append(" sendPostMessage.restResponseStatus != 200: ").append(restResponseStatus);

            int messageRetry_Count = MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    "sendPostMessage.POST.restResponseStatus != 200 ", false, null, MessageSend_Log);
            MessageSend_Log.error("[{}] sendPostMessage.POST.messageRetry_Count = {}", messageQueueVO.getQueue_Id(), messageRetry_Count);
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
            MessageSend_Log.error("[{}] sendPostMessage.POST.ApiRestHttpClient.close fault, Exception:{}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(IOE));
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
        MessageSend_Log.warn("[{}] sendPostMessage.POST.ApiRestHttpClient.close finally", messageQueueVO.getQueue_Id());
        if (ApiRestHttpClient != null)
            try {
                ApiRestHttpClient.close();

            } catch ( Exception IOE ) {
                MessageSend_Log.error("[{}] sendPostMessage.POST.ApiRestHttpClient.close finally fault, UnirestException:{}", messageQueueVO.getQueue_Id(), IOE.getMessage());
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

        MessageSend_Log.error("[{}] handle_Transport_Errors: {} ({}) fault, HttpRequestException:{}", messageQueueVO.getQueue_Id(), colledHttpMethodName, EndPointUrl, sStackTrace.strInterruptedException(e));
        messageDetails.MsgReason.append(" sendPostMessage.POST fault: ");
        if (e != null ) messageDetails.MsgReason.append(sStackTrace.strInterruptedException(e));

        // Журналируем UnirestException-ответ как есть
        if (IsDebugged)
            theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e), MessageSend_Log);

        // HE-4892 Если транспорт отвалился , то Шина ВСЁ РАВНО формирует как бы ответ , но с Fault внутри.
        // НАДО проверять количество порыток !!!
        MessageSend_Log.error("[{}] handle_Transport_Errors: {}: Retry_Count ({})>= ( ShortRetryCount={} LongRetryCount={})", messageQueueVO.getQueue_Id(),
                                            colledHttpMethodName, messageQueueVO.getRetry_Count(),
                                            messageDetails.MessageTemplate4Perform.getShortRetryCount(),
                                            messageDetails.MessageTemplate4Perform.getLongRetryCount());
        if (messageQueueVO.getRetry_Count() + 1 >= messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount()) {
            // количество порыток исчерпано, формируем результат для выхода из повторов
            MessageSend_Log.error("[{}] handle_Transport_Errors: {}(`{}`) make fault:{}", messageQueueVO.getQueue_Id(), colledHttpMethodName, EndPointUrl, sStackTrace.strInterruptedException(e) );
            append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, 506 ,
                                              colledHttpMethodName + " (`" + EndPointUrl+ "`) fault:" + e.getMessage()  ,
                                        null);
            MessageSend_Log.error("[{}] handle_Transport_Errors: XML_MsgResponse is:`{}`", messageQueueVO.getQueue_Id(), messageDetails.XML_MsgResponse.toString() );

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

    private static void do_Append_responseHttpHeaders_2_jSon(@NotNull StringBuilder JSON_MsgResponse, HttpHeaders responseHttpHeaders ) {
        // JSON_MsgResponse.append("{");
        if (responseHttpHeaders != null) {
            Map<String, List<String>> allHeaders = responseHttpHeaders.map();
            int i = 0;
            for (Map.Entry<String, List<String>> entry : allHeaders.entrySet() )
            { i = i+1;
                String httpHeader =  entry.getKey();
                if (httpHeader.equalsIgnoreCase("Path"))
                    httpHeader = httpHeader + i;
                JSON_MsgResponse.append("\"").append(httpHeader).append("\":");
                if (httpHeader.equals("set-cookie"))
                {
                    JSON_MsgResponse.append("{");
                }
                else {
                    JSON_MsgResponse.append("\"");
                }
                List<String> values = entry.getValue();
                for (String value : values)
                {
                    if (httpHeader.equals("set-cookie"))
                    {
                        String queryParams[];
                        queryParams = value.split(";");
                        // Set-Cookie: TOKEN=NjI3Y2FhZDQtM2U0Zi00YzNmLTk2NDYtZjNkYmQ5YTdhZDFl; Max-Age=1209600; Expires=Thu, 13 Nov 2025 23:35:01 GMT; Path=/; HttpOnly; SameSite=Lax
                        // TOKEN=OWNmYWZmZDYtMjBmZi00YzllLTg5MDYtYmRlMDBhY2Q4MzAy
                        // Max-Age=1209600
                        // Expires=Thu, 13 Nov 2025 22:50:15 GMT; Path=/; HttpOnly; SameSite=Lax
                        for (String queryParam : queryParams) {
                            i = i+1;
                            String ParamElements[] = queryParam.split("=");
                            if ((ParamElements.length > 1) && (ParamElements[1] != null)) {
                                if ( ParamElements[0].trim().equalsIgnoreCase("Path"))
                                JSON_MsgResponse.append("\"")
                                        .append(ParamElements[0].trim() ).append(i)
                                        .append("\":\"")
                                        .append(ParamElements[1]).append("\",");
                                else
                                    JSON_MsgResponse.append("\"")
                                            .append(ParamElements[0].trim())
                                            .append("\":\"")
                                            .append(ParamElements[1]).append("\",");

                            } else
                                JSON_MsgResponse.append("\"")
                                        .append(ParamElements[0].trim())
                                        .append("\":\"\",");
                        }
                    }
                    else
                    JSON_MsgResponse.append(
                            (value.trim().replace('"', ' ')) );
                }
                if (httpHeader.equals("set-cookie"))
                {
                    JSON_MsgResponse.append(" },");
                }
                else JSON_MsgResponse.append("\",");
            }
        }
        //JSON_MsgResponse.append("},");
    }

    private static StringBuilder append_Http_ResponseStatus_and_PlaneResponse ( @NotNull StringBuilder XML_MsgResponse, @NotNull Integer restResponseStatus,
                                                                                String restResponse, HttpHeaders responseHttpHeaders )
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
                        .append(XMLchars.Success_ExtResponse_HeadersString);
            if (responseHttpHeaders != null) {
                Map<String, List<String>> allHeaders = responseHttpHeaders.map();
                for (Map.Entry<String, List<String>> entry : allHeaders.entrySet() )
                { String httpHeader =  entry.getKey();
                    XML_MsgResponse.append(XMLchars.OpenTag).append(httpHeader).append(XMLchars.CloseTag);
                    List<String> values = entry.getValue();
                    for (String value : values)
                    { XML_MsgResponse.append(value).append("; "); }
                    XML_MsgResponse.append(XMLchars.OpenTag).append(XMLchars.EndTag).append(httpHeader).append(XMLchars.CloseTag);
                }
            }
            XML_MsgResponse.append(XMLchars.Success_ExtResponse_PayloadString);
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


/* НЕ нужен, сделан прасинг
    public static String getURL_from_Query_KEY_Value(@NotNull String saved_XML_MsgSEND, Document p_XMLdocument, Logger MessageSend_Log) throws JDOMException, IOException, XPathExpressionException {

        SAXBuilder documentBuilder;
        InputStream parsedConfigStream;
        Document XMLdocument;
        //  Если прарсинг ответа НЕ прошел, то тут уже псевдо-ответ от обработчика ошибки парсера
        if ( p_XMLdocument == null ) {
            documentBuilder = new SAXBuilder();
            parsedConfigStream = new ByteArrayInputStream(saved_XML_MsgSEND.getBytes(StandardCharsets.UTF_8));
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
                }
            }

            if ( !isSoapBodyFinded )
                throw new XPathExpressionException("getResponseBody: в SOAP-ответе не найден Element=" + XMLchars.Body);

        } else {
            throw new XPathExpressionException("getResponseBody: в SOAP-ответе не найден RootElement=" + XMLchars.Envelope);
        }

        return null;
    }
*/
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
