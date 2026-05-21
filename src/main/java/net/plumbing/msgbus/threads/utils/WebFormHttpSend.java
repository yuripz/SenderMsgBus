package net.plumbing.msgbus.threads.utils;

import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.json.JSONException;
import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.model.MessageTemplate4Perform;
import net.plumbing.msgbus.threads.TheadDataAccess;
import net.sf.saxon.s9api.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static net.plumbing.msgbus.threads.utils.MessageUtils.stripNonValidXMLCharacters;
//import static net.plumbing.msgbus.threads.utils.MessageHttpSend.handle_Transport_Errors;
import static net.plumbing.msgbus.threads.utils.MessageHttpSend.*;

public class WebFormHttpSend {

 // private static final String sBase64 ="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";
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



    private static byte[] readFileByPlaceholder( String inputStr, long Queue_Id, int ApiRestWaitTime,
                                                  MessageTemplate4Perform messageTemplate4Perform,
                                                  Logger MessageSend_Log ) throws IOException, SaxonApiException {


        int startIdx = inputStr.indexOf( XMLchars.URL_File_Path_Begin, 0);
        if (startIdx == -1) {
            // Нет маркеров, выходим как есть
            return inputStr.getBytes(StandardCharsets.UTF_8);
        }
        StringBuilder output = new StringBuilder();

        int currentIndex = 0;

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
            try {
                byte[] fileBytes = readBytesFromUrlWithAuthPreemptive(httpURLFilePath, ApiRestWaitTime, Queue_Id,
                        messageTemplate4Perform, MessageSend_Log);
                return fileBytes;

            } catch (IOException IOe) {
                MessageSend_Log.error("[{}][Ошибка при чтении URL:`{}`: {} ", Queue_Id, httpURLFilePath, IOe.getMessage());
                // В случае ошибки, генерим IOException
                throw new IOException ("Ошибка при чтении URL:`"+ httpURLFilePath +"`: "+ IOe.getMessage() + " , полученного для передачи вложений", IOe);
            }

            // НЕ Продвигаемся дальше, файл либо получили и вернули , либо вышли по IOException
            // currentIndex = endIdx + XMLchars.URL_File_Path_End.length();
        }

        return ("`[]`" + Queue_Id +"[Ошибка: в полученной строке `" + inputStr + "` не найден URL для получения файла").getBytes(StandardCharsets.UTF_8);
    }

    public static int sendWebFormMultiPart(@NotNull String saved_XML_MsgSEND, @NotNull Processor xPathProcessor, @NotNull XPathSelector xPathSelector,
                                         @NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess, int ApiRestWaitTime, Logger MessageSend_Log) {
        //
        MessageTemplate4Perform messageTemplate4Perform = messageDetails.MessageTemplate4Perform;

        StringBuilder EndPointUrl;
        String ROWID_QUEUElog=null;
        if ( StringUtils.substring(messageTemplate4Perform.getEndPointUrl(),0,"http".length()).equalsIgnoreCase("http") )
            EndPointUrl = new StringBuilder(messageTemplate4Perform.getEndPointUrl());
        else
            EndPointUrl = new StringBuilder("http://" + messageTemplate4Perform.getEndPointUrl());

        String formDataFieldName = ""; // messageQueueVO.getMsg_Type_own(); // по-умолчанию используем собственный тип
        List<ElementInfo> elements = processXml(xPathProcessor, xPathSelector,
                saved_XML_MsgSEND);
        boolean IsDebugged = messageDetails.MessageTemplate4Perform.getIsDebugged();
        for (ElementInfo info : elements) {
            if (IsDebugged ) {
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMultiPart.Элемент: {}", messageQueueVO.getQueue_Id(), info.elementName());
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMultiPart.Метка элемента: {}", messageQueueVO.getQueue_Id(), info.element().toString());
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMultiPart.formDataFieldName: {} ", messageQueueVO.getQueue_Id(), info.formDataFieldName());
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMultiPart.ContentType: {}", messageQueueVO.getQueue_Id(), info.contentType());
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMultiPart.filename: {}", messageQueueVO.getQueue_Id(), info.fileName());
                MessageSend_Log.warn("[{}] ElementInfo sendWebFormMultiPart.isJsonMadeManually: {}", messageQueueVO.getQueue_Id(), info.isJsonMadeManually());
            }
            if ( info.elementName().equalsIgnoreCase("Query_KEY_Value"))
            {
                EndPointUrl.append(info.element().getStringValue());
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
                MessageSend_Log.info("[{}] sendWebFormMultiPart.POST PropUser=`{}` PropPswd=`{}`", messageQueueVO.getQueue_Id(), PropUser, PropPswd);
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
                MessageSend_Log.info("[{}] sendWebFormMultiPart.POST PropUser== null (`{}`)", messageQueueVO.getQueue_Id(), PropUser);
            ApiRestHttpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .followRedirects(HttpClient.Redirect.ALWAYS)
                    .connectTimeout(Duration.ofSeconds(messageTemplate4Perform.getPropTimeout_Conn()))
                    .build();
        }
        StringBuilder RequestStringBody = new StringBuilder(messageDetails.XML_MsgSEND.length() + 128 );
         // 1. Используем List<byte[]> для последовательной передачи, в том числе и двоичных файлов
        List<byte[]> RequestByteBody = new ArrayList<>();

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
            // Поскольку XML с помощью xxx.MessageXSLT не метили тегами formDataFieldName, то преобразование к JSON должно быть в xxx.AckXSLT.
            if ((messageDetails.XML_MsgSEND.charAt(0) == '{') || (messageDetails.XML_MsgSEND.charAt(0) == '[')) {
                if (IsDebugged)
                    MessageSend_Log.info("[{}] sendWebFormMultiPart.POST JSON `{}`", messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND);
                try { // проверяем, получили ли валидный JSON после AckXSLT
                    JSONObject SendedJSONvalue = new JSONObject(messageDetails.XML_MsgSEND); // new JSONObject(kafkaRecord.value());

                    messageDetails.XML_MsgSEND = SendedJSONvalue.toString(0);
                    if (IsDebugged)
                        MessageSend_Log.info("[{}] sendWebFormMultiPartJSONObject: `{}`", messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND);

                } catch (JSONException jsonEx) {
                    MessageSend_Log.error("[{}] sendWebFormMultiPartJSONObject Ошибка при получении объекта как JSONObject, проверьте AckXSLT! - (`{}` , ...) return: {}",
                            messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND, jsonEx.getMessage());
                    return handle_Transport_Errors(theadDataAccess, messageQueueVO, messageDetails, EndPointUrl.toString(),
                            "sendWebFormMultiPart.POST, Ошибка при получении объекта как JSONObject", jsonEx,
                            ROWID_QUEUElog, IsDebugged, MessageSend_Log);
                }
            } else {
                MessageSend_Log.error("[{}] sendWebFormMultiPart.POST NOT JSON (XML с помощью .MessageXSLT не метили тегами formDataFieldName, то преобразование к JSON должно быть в .AckXSLT) `{}`",
                        messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND);
                // TO_DO: здесь надо бы формировать ошибку с руганью
                if (IsDebugged)
                    ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog(messageQueueVO.getQueue_Id(), messageDetails.XML_MsgSEND, MessageSend_Log);

                Exception e = new Exception("[" + messageQueueVO.getQueue_Id() + "] sendWebFormMultiPart.POST NOT JSON! ```\n" + messageDetails.XML_MsgSEND + "\n'```");
                MessageSend_Log.error("[{}] sendWebFormMultiPart: XML с помощью .MessageXSLT. не метили тегами formDataFieldName: `{}`", messageQueueVO.getQueue_Id(), e.getMessage());
                return handle_Transport_Errors(theadDataAccess, messageQueueVO, messageDetails, EndPointUrl.toString(), "sendWebFormMultiPart.POST", e,
                        ROWID_QUEUElog, IsDebugged, MessageSend_Log);
            }
        } // проверяем на JSon если одиночная форма и не метили тегами formDataFieldName, то преобразование к JSON должно быть в .AckXSLT.

        if (( messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty) == -1 )// NOT Header_is_empty
                && ( ! messageDetails.Soap_HeaderRequest.isEmpty() ))
        {
            headerParams = messageDetails.Soap_HeaderRequest.toString().split(":");
            if ( IsDebugged ) {
                MessageSend_Log.info("[{}] sendWebFormMultiPart.POST headerParams.length={}", messageQueueVO.getQueue_Id(), headerParams.length);
                for (int i = 0; i < headerParams.length; i++)
                    MessageSend_Log.info("[{}] sendWebFormMultiPart.POST headerParams[{}] = {}", messageQueueVO.getQueue_Id(), i, headerParams[i]);
            }
            if (headerParams.length > 1  )
                for (int i = 0; i < headerParams.length; i++)
                    httpHeaders.put(headerParams[0], headerParams[1]);
        }
        else { if ( IsDebugged )
            MessageSend_Log.info("[{}] sendWebFormMultiPart.POST indexOf(XMLchars.TagMsgHeaderEmpty(`Header_is_empty`))={}", messageQueueVO.getQueue_Id(),
                    messageDetails.Soap_HeaderRequest.indexOf(XMLchars.TagMsgHeaderEmpty));
        }
        // MessageSend_Log.info("[" + messageQueueVO.getQueue_Id() + "] sendPostMessage.Unirest.post `" + messageDetails.Soap_HeaderRequest + "` httpHeaders.size=" + httpHeaders.size() );
        //+                 "; headerParams= " + headerParams.toString() );
        try {

            boolean[] isReplaceContent4UrlPlaceholder = { false };

            // Используется только UTF-8 кодировка, прочие игнорируем
            boolean isUseBodyPublishers_ofString = false;
            if  (formDataFieldName.isEmpty()  ) //
            {     // messageQueueVO.getMsg_Type_own(); // по-умолчанию используем собственный тип
                MessageSend_Log.warn("[{}] RequestStringBody sendWebFormMultiPart.formDataFieldName: {} для {}, BO={} ", messageQueueVO.getQueue_Id(),
                        messageQueueVO.getMsg_Type_own(), messageQueueVO.getMsg_Type(), messageQueueVO.getOperation_Id());
                RequestStringBody.append("--").append(boundary).append("\r\n")
                        .append("Content-Disposition: form-data; name=\"")
                        .append(messageQueueVO.getMsg_Type_own())
                        .append("\"\r\n")
                        .append("Content-Type: application/json\r\n\r\n")
                ;
                if (messageDetails.XML_MsgSEND.indexOf(XMLchars.URL_File_Path_Begin) > 0) {
                    // здесь передаём Base64 содержание файла вместо URLBegin$$`...`$$EndURL
                    RequestStringBody.append(IOUtils.toString(MessageHttpSend.replaceUrlPlaceholders(messageDetails.XML_MsgSEND, isReplaceContent4UrlPlaceholder,
                                    messageQueueVO.getQueue_Id(), ApiRestWaitTime, messageTemplate4Perform, MessageSend_Log),
                            "UTF-8"));
                    }
                else
                    RequestStringBody.append(messageDetails.XML_MsgSEND);

                RequestStringBody.append("\r\n")
                        .append("--").append(boundary)
                        .append("--").append("\r\n")
                ;
                // в этой форме передаются только текстовые данные
                isUseBodyPublishers_ofString = true;
            }
            else { // для каждой секции с formDataFieldName формируем multipart
                isUseBodyPublishers_ofString = false;
                StringBuilder tmpRequestString4ByteBody = new StringBuilder(messageDetails.XML_MsgSEND.length() + 128 );
                for (ElementInfo info : elements) {
                    // очищаем tmpRequestString4ByteBody
                    tmpRequestString4ByteBody.setLength(0); tmpRequestString4ByteBody.trimToSize();

                    if ((info.formDataFieldName() != null) && ( !info.formDataFieldName().equalsIgnoreCase("null")) )
                    {
                        MessageSend_Log.warn("[{}] RequestByteBody sendWebFormMultiPart.Элемент: {}", messageQueueVO.getQueue_Id(), info.elementName() );
                        MessageSend_Log.warn("[{}] RequestByteBody sendWebFormMultiPart.formDataFieldName: {} " , messageQueueVO.getQueue_Id() , info.formDataFieldName());
                        MessageSend_Log.warn("[{}] RequestByteBody sendWebFormMultiPart.ContentType: {}", messageQueueVO.getQueue_Id() , info.contentType());
                        MessageSend_Log.warn("[{}] RequestByteBody sendWebFormMultiPart.fileName: {} " , messageQueueVO.getQueue_Id() , info.fileName());
                        MessageSend_Log.warn("[{}] RequestByteBody sendWebFormMultiPart.isJsonMadeManually: {}", messageQueueVO.getQueue_Id() , info.isJsonMadeManually());
                        formDataFieldName = info.formDataFieldName();
                        tmpRequestString4ByteBody.append("--").append(boundary).append("\r\n")
                                .append("Content-Disposition: form-data; name=\"")
                                .append( info.formDataFieldName() ).append("\"");
                        if (info.fileName() != null) {
                            tmpRequestString4ByteBody.append("; filename=\"").append( info.fileName() ).append("\"")
                                    .append("\r\n")
                                    .append("Content-Type: ").append(info.contentType()).append("\r\n")
                                    //.append("Content-Type: application/octet-stream\r\n")
                                    .append("\r\n"); //Важно! Content-Transfer-Encoding: binary или base64; -- убрали Content-Transfer-Encoding: binary
                        }
                        else {
                            tmpRequestString4ByteBody.append("\r\n")
                                    .append("Content-Type: ").append(info.contentType()).append("\r\n\r\n");
                        }
                        if ( info.contentType().contains("json") ) {
                            if ((info.isJsonMadeManually() !=null) && ( info.isJsonMadeManually().contains("true") )) {
                                MessageSend_Log.warn("[{}] RequestByteBody sendWebFormMultiPart.Тело элемента: {}", messageQueueVO.getQueue_Id(),
                                        messageDetails.XML_MsgSEND);
                                tmpRequestString4ByteBody.append( messageDetails.XML_MsgSEND)
                                        .append("\r\n");

                            }
                            else {
                                MessageSend_Log.warn("[{}] RequestByteBody sendWebFormMultiPart.Метка элемента: {}", messageQueueVO.getQueue_Id(), info.element().toString());
                                XdmNode parentNode = info.element();
                                // Предполагаем, что внутри один дочерний элемент
                                XdmNode childNode = null;
                                for (XdmSequenceIterator<XdmNode> it = parentNode.axisIterator(Axis.CHILD); ; ) {
                                    XdmNode child = it.next();
                                    childNode = child;
                                    break; // берем только первый
                                }
                                if (childNode != null) {
                                    tmpRequestString4ByteBody.append(
                                                    XML.toJSONObject(childNode.toString()).toString(0)
                                            )
                                            .append("\r\n")
                                    ;
                                } else
                                    tmpRequestString4ByteBody.append("[]")
                                            .append("\r\n")
                                            ;
                            }

                            RequestByteBody.add( (tmpRequestString4ByteBody.toString()).getBytes(StandardCharsets.UTF_8));

                        } else {
                            // данные не JSON
                            if (info.fileName() == null) {// text/plain
                                MessageSend_Log.warn("[{}] RequestByteBody sendWebFormMultiPart.содержимое элемента: {}", messageQueueVO.getQueue_Id(), info.element().getStringValue());
                                if (info.element().getStringValue().indexOf(XMLchars.URL_File_Path_Begin) > 0) {
                                    tmpRequestString4ByteBody.append(IOUtils.toString(MessageHttpSend.replaceUrlPlaceholders(info.element().getStringValue(), isReplaceContent4UrlPlaceholder,
                                                    messageQueueVO.getQueue_Id(), ApiRestWaitTime, messageTemplate4Perform, MessageSend_Log), "UTF-8"))
                                            .append("\r\n");
                                    }
                                else {
                                    tmpRequestString4ByteBody.append(info.element().getStringValue())
                                            .append("\r\n"); }
                                // добавляем данные
                                RequestByteBody.add( (tmpRequestString4ByteBody.toString()).getBytes(StandardCharsets.UTF_8));
                            }
                            else {
                                // добавляем данные о файле из заполненного tmpRequestString4ByteBody см. line 318-322
                                RequestByteBody.add( (tmpRequestString4ByteBody.toString()).getBytes(StandardCharsets.UTF_8));
                                // тут передаём именно содержимое файла, причём в Content-Transfer-Encoding: binary, Content-Type: application/octet-stream
                                MessageSend_Log.warn("[{}] RequestByteBody sendWebFormMultiPart.путь к файлу: {}", messageQueueVO.getQueue_Id(),
                                                        info.element().getStringValue());
                                try {

                                    byte[] fileBytesContent = readFileByPlaceholder(info.element().getStringValue(),
                                            messageQueueVO.getQueue_Id(), ApiRestWaitTime, messageTemplate4Perform, MessageSend_Log);
                                    //fileBytesContent = sBase64.getBytes(StandardCharsets.UTF_8); ///----!!!!!
                                    //fileBytesContent =Base64.getDecoder().decode(sBase64);

                                    RequestByteBody.add(fileBytesContent);
                                    RequestByteBody.add(("\r\n").getBytes(StandardCharsets.UTF_8));

                                    //RequestBody.append(fileContentISO_8859_1).append("\r\n");
                                        /*
                                        RequestBody.append(IOUtils.toString(replaceUrlPlaceholders(info.element().getStringValue(), isReplaceContent4UrlPlaceholder,
                                                        messageQueueVO.getQueue_Id(), ApiRestWaitTime, messageTemplate4Perform, MessageSend_Log), "UTF-8"))
                                                  .append("\r\n");
                                        */
                                } catch (Exception  sendIoExc) {
                                    MessageSend_Log.error("[{}] sendWebFormMultiPart.readFileByPlaceholder fault={}", messageQueueVO.getQueue_Id(),
                                            sStackTrace.strInterruptedException (sendIoExc));

                                    // Missing www-authenticate header when receiving 401 responses.
            /*
            As per section 4.1 of RFC-7235, when an HTTP server returns a 401 response, it must also return a WWW-Authenticate header :
            A server generating a 401 (Unauthorized) response MUST send a
            WWW-Authenticate header field containing at least one challenge.
            However, when the refinitiv server returns 401, it returns the following header :
            Authorization: WWW-Authenticate: Signature realm="World-Check One API",algorithm="hmac-sha256",headers="(request-target) host date content-type content-length"
             */
                                    System.err.println("[" + messageQueueVO.getQueue_Id() + "] sendWebFormMultiPart.POST ApiRestHttpClient.send IOException: `" + sendIoExc.getMessage() + "`");
                                    sendIoExc.printStackTrace();
                                    return handle_Transport_Errors ( theadDataAccess,  messageQueueVO,  messageDetails, EndPointUrl.toString(),  "sendWebFormMultiPart.POST", sendIoExc,
                                            ROWID_QUEUElog,  IsDebugged,   MessageSend_Log);
                                }
                                // записываем байты
                                // в конце (после каждого файла) добавим \r\n
                                // Для этого лучше собрать байтовый массив, но для простоты используем StringBuilder с encode
                                // или создаем массив байтов вручную позже

                            }
                        }

                    }
                }// for elements
                // tmpRequestString4ByteBody.append("--").append(boundary).append("--").append("\r\n");
                RequestByteBody.add( ("--" + boundary + "--" + "\r\n")
                                    .getBytes(StandardCharsets.UTF_8));
            }
            // = ( formDataFieldName + "=" + URLEncoder.encode( messageDetails.XML_MsgSEND, StandardCharsets.UTF_8) );

            try {
                messageDetails.Confirmation.clear();
                messageDetails.XML_MsgResponse.setLength(0);

                if ( IsDebugged ) {
                    if ( isUseBodyPublishers_ofString ) {
                        MessageSend_Log.info("[{}] sendWebFormMultiPart.formDataFieldName as  String`{}` to (`{}`)",
                                messageQueueVO.getQueue_Id(), RequestStringBody, EndPointUrl.toString());
                    }
                    else {
                        RequestStringBody.setLength(0);
                        RequestStringBody.trimToSize();
                        for (byte[] byteArray : RequestByteBody) {
                            if (byteArray != null) {  // Обработка случая, если byteArray может быть null
                                String chunk = new String(byteArray, StandardCharsets.UTF_8);
                                RequestStringBody.append(chunk);
                            }
                            MessageSend_Log.info("[{}] sendWebFormMultiPart.formDataFieldName as  List<byte[]> `{}` to (`{}`)",
                                    messageQueueVO.getQueue_Id(), RequestStringBody, EndPointUrl.toString());
                        }
                    }
                        ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog(messageQueueVO.getQueue_Id(), RequestStringBody.toString(), MessageSend_Log);
                        MessageSend_Log.info("[{}] sendWebFormMultiPart UTL to (`{}`).connectTimeoutInMillis={};.readTimeoutInMillis=ReadTimeoutInMillis= {} PropUser:{}",
                                messageQueueVO.getQueue_Id(), EndPointUrl.toString(), messageTemplate4Perform.getPropTimeout_Conn(), messageTemplate4Perform.getPropTimeout_Read(), PropUser);

                }

                HttpRequest.Builder requestBuilder = java.net.http.HttpRequest.newBuilder();
                // добавляем все заголовки как есть через HttpRequest.Builder
                for (Map.Entry<String, String> entry: httpHeaders.entrySet()) {
                    requestBuilder = requestBuilder
                            .header(entry.getKey(),entry.getValue());
                    if ( IsDebugged )
                        MessageSend_Log.info("[{}] sendWebFormMultiPart.POST .header: `{}:{}`", messageQueueVO.getQueue_Id(), entry.getKey(), entry.getValue());
                    // queryString.append(entry.getKey()).append("=").append(entry.getValue());
                }

                java.net.http.HttpRequest request;
                if ( isUseBodyPublishers_ofString )
                    request = requestBuilder
                                    .POST( HttpRequest.BodyPublishers.ofString(RequestStringBody.toString()) )
                                    .uri(URI.create(EndPointUrl.toString()))
                                    .timeout( Duration.ofSeconds( messageTemplate4Perform.getPropTimeout_Read()) )
                                    .build();
                else
                    request = requestBuilder
                                    .POST(HttpRequest.BodyPublishers.ofByteArrays(RequestByteBody))
                                    .uri(URI.create(EndPointUrl.toString()))
                                    .timeout( Duration.ofSeconds( messageTemplate4Perform.getPropTimeout_Read()) )
                                    .build();
                HttpResponse<String> Response= null;
                try {
                    Response= ApiRestHttpClient.send(request, HttpResponse.BodyHandlers.ofString() );

                } catch (IOException sendIoExc) {
                    MessageSend_Log.error("[{}] sendWebFormMultiPart.ApiRestHttpClient (.isTerminated=`{}`).send fault={}", messageQueueVO.getQueue_Id(),
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
                    System.err.println("[" + messageQueueVO.getQueue_Id() + "] sendWebFormMultiPart.POST ApiRestHttpClient.send IOException: `" + sendIoExc.getMessage() + "`");
                    sendIoExc.printStackTrace();
                    return handle_Transport_Errors ( theadDataAccess,  messageQueueVO,  messageDetails, EndPointUrl.toString(),  "sendWebFormMultiPart.POST", sendIoExc,
                            ROWID_QUEUElog,  IsDebugged,   MessageSend_Log);
                }

                restResponseStatus = Response.statusCode();

                //Test = Response.getBody();
                // HttpHeaders responseHttpHeaders = null;
                HttpHeaders responseHttpHeaders = Response.headers();
                //MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Response getHeaders()=" + headers.all().toString() +" getHeaders().size=" + headers.size() );

                MessageSend_Log.warn("[{}] sendWebFormMultiPart.Response httpCode={} getBody().length={}", messageQueueVO.getQueue_Id(), restResponseStatus, Response.body().length());
                // MessageSend_Log.warn("[" + messageQueueVO.getQueue_Id() + "]" +"sendPostMessage.Response getBody()=" + Arrays.toString(Test) +" getBody().length=" + Test.length );

                if ( restResponseStatus == 200)
                {
                    String[] payloadResponsePartLines = Response.body().split("\n", 2);
                    if (payloadResponsePartLines.length < 1)
                    {
                        System.err.println("Неверный формат ответа");

                        Exception e = new Exception(" sendWebFormMultiPart.Response lines.length=" + Integer.toString( payloadResponsePartLines.length  ) + "\n" + Response.body() );
                        if (IsDebugged)
                            MessageSend_Log.error("[{}] sendWebFormMultiPart call handle_Transport_Errors: `{}`", messageQueueVO.getQueue_Id(), e.getMessage());
                        return handle_Transport_Errors(theadDataAccess, messageQueueVO, messageDetails, EndPointUrl.toString(), "sendWebFormMultiPart.POST", e,
                                ROWID_QUEUElog, IsDebugged, MessageSend_Log);
                    }
                    // String firstLine = lines[0]; // например, "33"
                    // String payloadResponsePart = payloadResponsePartLines[1];
                    // вторая строка содержит полезную нагрузку
                    RestResponse = stripNonValidXMLCharacters( payloadResponsePartLines[0] ); // StandardCharsets.UTF_8);
                }
                else {
                    MessageSend_Log.warn("[{}] sendWebFormMultiPart.Response httpCode !=200 ={}, getBody()=`{}` responseHttpHeaders{}",
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
                    Exception e = new Exception(" sendWebFormMultiPart.Response httpCode=" + Integer.toString(restResponseStatus  ) + "\n" + RestResponse );
                    if (IsDebugged)
                        MessageSend_Log.error("[{}] sendWebFormMultiPart call handle_Transport_Errors: `{}`", messageQueueVO.getQueue_Id(), e.getMessage());
                    return handle_Transport_Errors(theadDataAccess, messageQueueVO, messageDetails, EndPointUrl.toString(), "sendWebFormMultiPart.POST", e,
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
                            MessageHttpSend.do_Append_responseHttpHeaders_2_jSon( RestResponse_with_HttpResponseStatusCode, responseHttpHeaders );
                            RestResponse_with_HttpResponseStatusCode.append(" },");
                            RestResponse_with_HttpResponseStatusCode.append("\"payload\":")
                                    .append(RestResponse)
                                    .append("}");
                            if (IsDebugged)
                                MessageSend_Log.info("[{}] sendWebFormMultiPart.POST RestResponseJSON=({})", messageQueueVO.getQueue_Id(), RestResponse_with_HttpResponseStatusCode.toString());

                            JSONObject RestResponseJSON = new JSONObject( RestResponse_with_HttpResponseStatusCode.toString() );
                            String XML_MsgResponse_Body = XML.toString(RestResponseJSON, XMLchars.NameRootTagContentJsonResponse);
                            if (IsDebugged)
                                MessageSend_Log.info("[{}] sendWebFormMultiPart.POST XML_MsgResponse_Body=({})", messageQueueVO.getQueue_Id(), XML_MsgResponse_Body);

                            messageDetails.XML_MsgResponse.append( XML_MsgResponse_Body );
                            messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
                            messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);
                            if (IsDebugged)
                                theadDataAccess.doUPDATE_QUEUElog(ROWID_QUEUElog, messageQueueVO.getQueue_Id(), RestResponse_with_HttpResponseStatusCode.toString(), MessageSend_Log);
                        } catch (Exception JSONe) { // получили непонятно что
                            MessageSend_Log.error("[{}] sendWebFormMultiPart.POST Exception JSONe получили непонятно что=({})", messageQueueVO.getQueue_Id(),
                                    JSONe.getMessage());

                            // Кладем полученный ответ в <MsgData><![CDATA[" RestResponse "]]></MsgData>
                            append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse, responseHttpHeaders );
                        }

                    } else {
                        MessageSend_Log.error("[{}] sendWebFormMultiPart.POST UNKNOWN ответ и не `{` и не `<` - опять же получили непонятно что=({})", messageQueueVO.getQueue_Id(),
                                RestResponse);
                        // ответ и не `{` и не `<` - опять же получили непонятно что
                        // Кладем полученный ответ в <MsgData><![CDATA[" RestResponse "]]></MsgData>
                        append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse, responseHttpHeaders );
                    }
                }

                if (IsDebugged)
                    MessageSend_Log.info("[{}] sendWebFormMultiPart.POST Envelope_MsgResponse=({})", messageQueueVO.getQueue_Id(), messageDetails.XML_MsgResponse.toString());


                // -- Задваивается в случае ошибки => это делается внутри ProcessingSendError()
                // messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);

            } catch (Exception e) {
                return handle_Transport_Errors ( theadDataAccess,  messageQueueVO,  messageDetails, EndPointUrl.toString(),  "sendPostMessage.POST", e,
                        ROWID_QUEUElog,  IsDebugged,   MessageSend_Log);

            }

            if (IsDebugged)
                MessageSend_Log.info("[{}] sendWebFormMultiPart.POST httpStatus=[{}], RestResponse=({})", messageQueueVO.getQueue_Id(), restResponseStatus, RestResponse);

            try {
                // Получили ответ от сервиса, инициируем обработку getResponseBody()
                InputStream parsedRestResponseStream;
                parsedRestResponseStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
                SAXBuilder documentBuilder = new SAXBuilder();
                Document XMLdocument;

                try {
                    XMLdocument = documentBuilder.build(parsedRestResponseStream);
                    if (IsDebugged)
                        MessageSend_Log.info("[{}] sendWebFormMultiPart documentBuilder info=`{}`, XML_MsgResponse=({})",
                                messageQueueVO.getQueue_Id(), XMLdocument.toString(), messageDetails.XML_MsgResponse);

                } catch (JDOMException RestResponseE) {
                    XMLdocument = null;
                    MessageSend_Log.error("[{}]sendWebFormMultiPart.documentBuilder fault: {}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(RestResponseE));
                    // формируем искуственный XML_MsgResponse из Fault ,  меняем XML_MsgResponse
                    append_Http_ResponseStatus_and_PlaneResponse( messageDetails.XML_MsgResponse, restResponseStatus , RestResponse, null );
                }

                MessageSoapSend.getResponseBody(messageDetails, XMLdocument, MessageSend_Log);

                if (IsDebugged)
                    MessageSend_Log.info("[{}] sendWebFormMultiPart:ClearBodyResponse=({})", messageQueueVO.getQueue_Id(), messageDetails.XML_ClearBodyResponse.toString());
                // client.wait(100);

            } catch (Exception e) {
                System.err.println("[" + messageQueueVO.getQueue_Id() + "]  Exception");
                e.printStackTrace();
                MessageSend_Log.error("[{}] sendWebFormMultiPart.getResponseBody fault: {}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(e));
                messageDetails.MsgReason.append(" sendWebFormMultiPart.getResponseBody fault: ").append(sStackTrace.strInterruptedException(e));

                MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendWebFormMultiPart.getResponseBody", true, e, MessageSend_Log);
                return -3;
            }
            if (restResponseStatus != 200) // Rest вызов считаем успешным только при получении
            {
                MessageSend_Log.error("[{}] sendWebFormMultiPart.restResponseStatus != 200: {}", messageQueueVO.getQueue_Id(), restResponseStatus);
                messageDetails.MsgReason.append(" sendWebFormMultiPart.restResponseStatus != 200: ").append(restResponseStatus);

                int messageRetry_Count = MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                        "sendWebFormMultiPart.restResponseStatus != 200 ", false, null, MessageSend_Log);
                MessageSend_Log.error("[{}] sendWebFormMultiPart.messageRetry_Count = {}", messageQueueVO.getQueue_Id(), messageRetry_Count);
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
                    MessageSend_Log.error("[{}] sendWebFormMultiPart.ApiRestHttpClient.close fault, Exception:{}", messageQueueVO.getQueue_Id(), sStackTrace.strInterruptedException(IOE));
                }
            ApiRestHttpClient = null;

        } finally {
            MessageSend_Log.warn("[{}] sendWebFormMultiPart.ApiRestHttpClient.close finally", messageQueueVO.getQueue_Id());
            if (ApiRestHttpClient != null)
                try {
                    ApiRestHttpClient.close();

                } catch ( Exception IOE ) {
                    MessageSend_Log.error("[{}] sendWebFormMultiPart.ApiRestHttpClient.close finally fault, UnirestException:{}", messageQueueVO.getQueue_Id(), IOE.getMessage());
                }
            ApiRestHttpClient = null;
        }
        return 0;
    }

}
