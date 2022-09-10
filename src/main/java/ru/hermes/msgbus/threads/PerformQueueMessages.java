package ru.hermes.msgbus.threads;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.lang3.StringUtils;
//import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.params.ConnRouteParams;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeLayeredSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
//import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import ru.hermes.msgbus.common.XMLchars;
import ru.hermes.msgbus.common.sStackTracе;
import ru.hermes.msgbus.common.xlstErrorListener;
import ru.hermes.msgbus.model.*;
import ru.hermes.msgbus.monitoring.ConcurrentQueue;
import ru.hermes.msgbus.threads.utils.*;
import ru.hermes.msgbus.ws.client.SoapClientException;
import ru.hermes.msgbus.ws.client.core.Security;
import ru.hermes.msgbus.ws.client.ssl.SSLUtils;
import ru.hermes.msgbus.threads.utils.MessageRepositoryHelper;

import javax.net.ssl.SSLContext;
import javax.validation.constraints.NotNull;
import javax.xml.XMLConstants;
import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;


import static ru.hermes.msgbus.common.sStackTracе.strInterruptedException;
import static ru.hermes.msgbus.ws.client.core.SoapConstants.HTTPS;

import ru.hermes.msgbus.common.xlstErrorListener;
public class PerformQueueMessages {

    private DefaultHttpClient client=null;
    private CloseableHttpClient httpClient=null;
    private xlstErrorListener XSLTErrorListener=null;
    private ThreadSafeClientConnManager ExternalConnectionManager;
    private String ConvXMLuseXSLTerr = "";
  //  org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;

    public void setExternalConnectionManager( ThreadSafeClientConnManager externalConnectionManager ) {
        this.ExternalConnectionManager = externalConnectionManager;
    }
    public void setConvXMLuseXSLTerr( String p_ConvXMLuseXSLTerr) {
        this.ConvXMLuseXSLTerr = p_ConvXMLuseXSLTerr;
    }

    public  long performMessage(MessageDetails Message, MessageQueueVO messageQueueVO, TheadDataAccess theadDataAccess, Logger MessegeSend_Log) {
        // 1. Получаем шаблон обработки для MessageQueueVO
        String SubSys_Cod = messageQueueVO.getSubSys_Cod();
        int MsgDirection_Id = messageQueueVO.getMsgDirection_Id();
        int Operation_Id = messageQueueVO.getOperation_Id();
        Long Queue_Id = messageQueueVO.getQueue_Id();
        String Queue_Direction = messageQueueVO.getQueue_Direction();
        String AnswXSLTQueue_Direction=Queue_Direction;


        //int Max_Retry_Count = 1;
        //int Max_Retry_Time = 30;
        String URL_SOAP_Send = "";
        int Function_Result = 0;

        XSLTErrorListener = new xlstErrorListener();
        XSLTErrorListener.setXlstError_Log( MessegeSend_Log );

        MonitoringQueueVO monitoringQueueVO = new MonitoringQueueVO();

        MessegeSend_Log.info(Queue_Direction + " [" + Queue_Id + "] ищем Шаблон под оперрацию (" + Operation_Id + "), с учетом системы приёмника MsgDirection_Id=" + MsgDirection_Id + ", SubSys_Cod =" + SubSys_Cod);

        // ищем Шаблон под оперрацию, с учетом системы приёмника ru.hermes.msgbus.threads.utils.MessageRepositoryHelper.look4MessageTemplateVO_2_Perform
        int Template_Id = MessageRepositoryHelper.look4MessageTemplateVO_2_Perform(Operation_Id, MsgDirection_Id, SubSys_Cod, MessegeSend_Log);
        MessegeSend_Log.info(Queue_Direction + " [" + Queue_Id + "]  Шаблон под оперрацию =" + Template_Id);

        if ( Template_Id < 0 ) {
            theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO , "Не нашли шаблон обработки сообщения для комбинации: Идентификатор системы[" + MsgDirection_Id + "] Код подсистемы[" + SubSys_Cod + "]", MessegeSend_Log);
            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()), monitoringQueueVO, MessegeSend_Log);
            return -11L;
        }

        int messageTypeVO_Key = MessageRepositoryHelper.look4MessageTypeVO_2_Perform( Operation_Id, MessegeSend_Log );
        if ( messageTypeVO_Key < 0  ) {
            MessegeSend_Log.error( "[" + Queue_Id + "] MessageRepositoryHelper.look4MessageTypeVO_2_Perform: Не нашли тип сообщения для Operation_Id=[" + Operation_Id + "]");
            theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO, "Не нашли тип сообщения для Operation_Id=[" + Operation_Id + "]", MessegeSend_Log);
            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessegeSend_Log);
            return -11L;
        }
        URL_SOAP_Send = MessageType.AllMessageType.get(messageTypeVO_Key).getURL_SOAP_Send();

        /*for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
            if ( messageTypeVO.getOperation_Id() == Operation_Id ) {    //  нашли операцию,
                Max_Retry_Count = messageTypeVO.getMax_Retry_Count();
                Max_Retry_Time = messageTypeVO.getMax_Retry_Time();
                URL_SOAP_Send = messageTypeVO.getURL_SOAP_Send();
            }
        }*/

        int MsgDirectionVO_Key = MessageRepositoryHelper.look4MessageDirectionsVO_2_Perform(MsgDirection_Id, SubSys_Cod, MessegeSend_Log);

        if ( MsgDirectionVO_Key >= 0 )
            MessegeSend_Log.info(
                    "[" + Queue_Id + "] MsgDirectionVO  getDb_pswd=" + MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getDb_pswd() +
                            MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).LogMessageDirections());
        else {
            MessegeSend_Log.error(Queue_Direction +" ["+ Queue_Id +"] Не нашли систему-приёмник для пары[" + MsgDirection_Id + "][" + SubSys_Cod + "]" );
            if ( theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO, "Не нашли систему-приёмник для пары[" + MsgDirection_Id + "][" + SubSys_Cod + "]", MessegeSend_Log) < 0 )
            {   ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessegeSend_Log);
                return -11L;
            }
            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessegeSend_Log);
            return -12L;
        }


        Message.MessageTemplate4Perform = new MessageTemplate4Perform(MessageTemplate.AllMessageTemplate.get(Template_Id),
                URL_SOAP_Send, //  хвост для добавления к getWSDL_Name() из MessageDirections
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getWSDL_Name(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getDb_user(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getDb_pswd(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getType_Connect(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getShort_retry_count(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getShort_retry_interval(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getLong_retry_count(),
                MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getLong_retry_interval(),
                Queue_Id,
                MessegeSend_Log
        );
        MessegeSend_Log.info("[" + Queue_Id + "] MessageTemplate4Perform[" + Message.MessageTemplate4Perform.printMessageTemplate4Perform() );

        CloseableHttpClient SimpleHttpClient = null; // Message.SimpleHttpClient;
/*        if ( SimpleHttpClient != null ) {
            try {
                SimpleHttpClient.close();
            } catch (IOException e) {
                MessegeSend_Log.error("и еще ошибка SimpleHttpClient.close(): " + e.getMessage());
                return -22L;
            }
        }
*/


        String isProxySet = System.getProperty("http.proxySet");
        if ( (isProxySet != null) && (isProxySet.equals("true")) )
            SimpleHttpClient = getProxedHttpClient(
                    Message.sslContext, Message.MessageTemplate4Perform,
                    //Message.MessageTemplate4Perform.getPropTimeout_Read() * 1000,
                    //Message.MessageTemplate4Perform.getPropTimeout_Conn() * 1000,
                    MessegeSend_Log
            );
            else
        SimpleHttpClient = getCloseableHttpClient(
                Message.sslContext, Message.MessageTemplate4Perform,
                //Message.MessageTemplate4Perform.getPropTimeout_Read() * 1000,
                //Message.MessageTemplate4Perform.getPropTimeout_Conn() * 1000,
                MessegeSend_Log
        );


        if ( SimpleHttpClient == null) {
            theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO, "внутренняя ошибка - не смогли создать CloseableHttpClient ]", MessegeSend_Log);
            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessegeSend_Log);
            return -22L;
        }
        Message.SimpleHttpClient = SimpleHttpClient;
        /* - Exception java.lang.UnsupportedOperationException
        MessageHttpSend.setHttpConnectionParams( SimpleHttpClient,
                Message.MessageTemplate4Perform.getPropTimeout_Read() * 1000, Message.MessageTemplate4Perform.getPropTimeout_Conn() * 1000
        );*/


        switch (Queue_Direction){
            case XMLchars.DirectOUT:
                // читаем их БД тело XML
                MessegeSend_Log.info(Queue_Direction +" ["+ Queue_Id +"] читаем из БД тело XML" );
                MessageUtils.ReadMessage( theadDataAccess, Queue_Id, Message, MessegeSend_Log);
                if ( Message.MessageTemplate4Perform.getMessageXSD() != null )
                { boolean is_Message_OUT_Valid;
                    is_Message_OUT_Valid = TestXMLByXSD( Message.XML_MsgOUT.toString(), Message.MessageTemplate4Perform.getMessageXSD(), Message.MsgReason, MessegeSend_Log );
                    if ( ! is_Message_OUT_Valid ) {
                        MessegeSend_Log.error(" ["+ Queue_Id +"] validateXMLSchema: message\n" + Message.XML_MsgOUT.toString() + "\n is not valid for XSD\n" + Message.MessageTemplate4Perform.getMessageXSD());
                        MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess,
                                "validateXMLSchema: message\n" + Message.XML_MsgOUT.toString() + "\n is not valid for XSD\n" + Message.MessageTemplate4Perform.getMessageXSD() ,
                                null ,  MessegeSend_Log);
//                        try {  SimpleHttpClient.close(); } catch ( IOException e) {
//                            MessegeSend_Log.error("и еще ошибка SimpleHttpClient.close(): " + e.getMessage() );
                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, // Message.XML_MsgOUT.toString(),
                                null, // Message.MessageTemplate4Perform.getMessageXSD(),
                                 monitoringQueueVO, MessegeSend_Log);
                        return -1L;
                    }
                }
                // преобразовываем тело
                String MessageXSLT_4_OUT_2_SEND = Message.MessageTemplate4Perform.getMessageXSLT();
                if ( MessageXSLT_4_OUT_2_SEND != null ) {
                    String XML_4_XSLT;
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessegeSend_Log.info(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь тела:{" + MessageXSLT_4_OUT_2_SEND +"}");
                        // если в ConfigExecute SearchString и Replacement заданы, то заменяем!
                    if (( Message.MessageTemplate4Perform.getPropSearchString() != null ) && ( Message.MessageTemplate4Perform.getPropReplacement() != null ))
                    {
                        if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessegeSend_Log.info(Queue_Direction + " [" + Queue_Id + "] SearchString:{" + Message.MessageTemplate4Perform.getPropSearchString() +"}, Replacement:{" + Message.MessageTemplate4Perform.getPropReplacement() +"}");
                        XML_4_XSLT = StringUtils.replace( Message.XML_MsgOUT.toString(),
                                Message.MessageTemplate4Perform.getPropSearchString(),
                                Message.MessageTemplate4Perform.getPropReplacement(),
                                -1);
                    }
                    else XML_4_XSLT = Message.XML_MsgOUT.toString();
                    try {
                        Message.XML_MsgSEND = ConvXMLuseXSLT(Queue_Id, XML_4_XSLT, // Message.XML_MsgOUT.toString(),
                                MessageXSLT_4_OUT_2_SEND, Message.MsgReason,
                                MessegeSend_Log, Message.MessageTemplate4Perform.getIsDebugged()
                        ).substring(XMLchars.xml_xml.length());// берем после <?xml version="1.0" encoding="UTF-8"?>
                        if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessegeSend_Log.info(Queue_Direction + " [" + Queue_Id + "] после XSLT=:{" + Message.XML_MsgSEND + "}");
                    } catch ( TransformerException exception ) {
                        MessegeSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь тела:{" + MessageXSLT_4_OUT_2_SEND +"}");
                        MessegeSend_Log.error(Queue_Direction + " [" + Queue_Id + "] после XSLT=:{" + Message.XML_MsgSEND +"}");
                        MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess,
                                "XSLT fault message: " + ConvXMLuseXSLTerr + XML_4_XSLT+ " on " + MessageXSLT_4_OUT_2_SEND ,
                                null ,  MessegeSend_Log);
                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, //XML_4_XSLT, // Message.XML_MsgOUT.toString(),
                                null, //MessageXSLT_4_OUT_2_SEND,
                                  monitoringQueueVO, MessegeSend_Log);
                        return -2L;
                    }
                    if ( Message.XML_MsgSEND.equals(XMLchars.nanXSLT_Result) ) {
                        MessegeSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь тела:{" + MessageXSLT_4_OUT_2_SEND +"}");
                        MessegeSend_Log.error(Queue_Direction + " [" + Queue_Id + "] после XSLT=:{" + Message.XML_MsgSEND +"}");
                        MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess,
                                "XSLT fault message: " + ConvXMLuseXSLTerr + XML_4_XSLT + " on " + MessageXSLT_4_OUT_2_SEND ,
                                null ,  MessegeSend_Log);
                       // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, XML_4_XSLT, MessageXSLT_4_OUT_2_SEND,  monitoringQueueVO, MessegeSend_Log);
                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                        return -201L;
                    }

                     // сохраняем результат XSLT-преобразования( body ) распарсенный по-строчно <Tag><VALUE>
                    if ( MessageUtils.ReplaceMessage4SEND( theadDataAccess, Queue_Id, Message, messageQueueVO, MessegeSend_Log)  < 0 )
                    { // Результат преобразования не получилось записать в БД
                        //HE-5864 Спец.символ UTF-16 или любой другой invalid XML character . Ошибка при отправке - удаляет и не записывант сообщение по
                       // Внутри ReplaceMessage4SEND вызов MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess, "ReplaceMessage4SEND fault" + Message.XML_MsgSEND  ,ex,  MessegeSend_Log);
                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, XML_4_XSLT, Message.XML_MsgSEND,  monitoringQueueVO, MessegeSend_Log);
                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                        return -202L;
                    }
                }       //   MessageXSLT_4_OUT_2_SEND != null
                else
                {  // что на входе, то и отправляем, если нет MessageXSLT для преобразования
                    // ! но если в ConfigExecute SearchString и Replacement заданы, то заменяем!
                    if (( Message.MessageTemplate4Perform.getPropSearchString() != null ) && ( Message.MessageTemplate4Perform.getPropReplacement() != null )) {
                        if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessegeSend_Log.info(Queue_Direction + " [" + Queue_Id + "] SearchString:{" + Message.MessageTemplate4Perform.getPropSearchString() +"}, Replacement:{" + Message.MessageTemplate4Perform.getPropReplacement() +"}");

                        Message.XML_MsgSEND = StringUtils.replace( Message.XML_MsgOUT.toString(),
                                Message.MessageTemplate4Perform.getPropSearchString(),
                                Message.MessageTemplate4Perform.getPropReplacement(),
                                -1);
                    }
                    else
                    Message.XML_MsgSEND = Message.XML_MsgOUT.toString();
                }

                // устанавливаем признак "SEND" & COMMIT
                if ( theadDataAccess.doUPDATE_MessageQueue_Out2Send( messageQueueVO, "XSLT (OUT) -> (SEND) ok",  MessegeSend_Log) < 0 )
                {
                    // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_MsgOUT.toString(), messageQueueVO.getMsg_Reason(),  monitoringQueueVO, MessegeSend_Log);
                    ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                    return -203L;
                }
                else
                    // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_MsgOUT.toString(), Message.XML_MsgSEND,  monitoringQueueVO, MessegeSend_Log);
                    ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);

            case XMLchars.DirectSEND:
                if ( !Queue_Direction.equals("OUT") ) {
                    // надо читать из БД
                    MessegeSend_Log.info(Queue_Direction +"-> SEND ["+ Queue_Id +"] читаем SEND БД тело XML" );
                    MessageUtils.ReadMessage( theadDataAccess, Queue_Id, Message, MessegeSend_Log);
                    if ( Message.MessageRowNum <= 0 ) {
                        MessegeSend_Log.error(Queue_Direction +"-> SEND ["+ Queue_Id +"] тело XML для SEND в БД пустое !" );
                        MessageUtils.ProcessingOutError(  messageQueueVO,   Message,  theadDataAccess,
                                Queue_Direction +"-> SEND ["+ Queue_Id +"] тело XML для SEND в БД пустое !" ,
                                null ,  MessegeSend_Log);
                        // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_MsgSEND, Queue_Direction +"-> SEND ["+ Queue_Id +"] тело XML для SEND в БД пустое !",  monitoringQueueVO, MessegeSend_Log);
                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                        return -3L;
                    }

                    // Если дата согздания до секунд совпала с датой 1-го SEND, значит её ещё не установливали
                    if ( messageQueueVO.getQueue_Create_Date().equals( messageQueueVO.getQueue_Date() ) ) {
                        if ( theadDataAccess.doUPDATE_MessageQueue_Queue_Date4Send(messageQueueVO, MessegeSend_Log) < 0 )
                            return -4L;
                    }

                    Message.XML_MsgSEND = Message.XML_MsgOUT.toString();
                    // Queue_Direction = "SEND";
                }
                Queue_Direction = XMLchars.DirectSEND;
                messageQueueVO.setQueue_Direction(XMLchars.DirectSEND);
                // if ( Queue_Direction.equalsIgnoreCase( "SEND") ) break; // ИЩЕМ Утечку потоков !!!

                // вызов внешней системы
                // провевяем , вдруг это REST
                MessegeSend_Log.info("doSEND ["+ Queue_Id +"] getPropWebMetod=" + Message.MessageTemplate4Perform.getPropWebMetod() +
                        " EndPointUrl=" + Message.MessageTemplate4Perform.getEndPointUrl() +
                        " PropTimeout_Conn=" + Message.MessageTemplate4Perform.getPropTimeout_Conn() +
                        " PropTimeout_Read=" + Message.MessageTemplate4Perform.getPropTimeout_Read() +
                        " Type_Connection=" + Message.MessageTemplate4Perform.getType_Connection() +
                        " ShortRetryCount=" + Message.MessageTemplate4Perform.getShortRetryCount() +
                        " LongRetryCount=" + Message.MessageTemplate4Perform.getLongRetryCount());


                if ( Message.MessageTemplate4Perform.getPropWebMetod() != null ) {
                    if ( Message.MessageTemplate4Perform.getPropWebMetod().equals("get")) {
                        Function_Result = MessageHttpSend.HttpGetMessage(messageQueueVO, Message, theadDataAccess,  MessegeSend_Log);
                    }
                    if ( Message.MessageTemplate4Perform.getPropWebMetod().equals("post")) {
                        Function_Result = MessageHttpSend.sendPostMessage(messageQueueVO, Message, theadDataAccess,  MessegeSend_Log);
                    }
                    if ( ( !Message.MessageTemplate4Perform.getPropWebMetod().equals("get") ) &&
                         ( !Message.MessageTemplate4Perform.getPropWebMetod().equals("post")) )
                    {
                        MessageUtils.ProcessingSendError(  messageQueueVO,   Message,  theadDataAccess,
                                "Свойство WebMetod["+ Message.MessageTemplate4Perform.getPropWebMetod() + "], указаное в шаблоне не 'get' и не 'post'", true,
                                null ,  MessegeSend_Log);
                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_MsgSEND, "Свойство WebMetod["+ Message.MessageTemplate4Perform.getPropWebMetod() + "], указаное в шаблоне не 'get' и не 'post'",  monitoringQueueVO, MessegeSend_Log);
                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                        return -401L;
                    }
                }
                else {
                    // готовим заголовок

                    Message.Soap_HeaderRequest.setLength(0);
                    if ( Message.MessageTemplate4Perform.getHeaderXSLT() != null && Message.MessageTemplate4Perform.getHeaderXSLT().length() > 10 ) // Есть чем преобразовывать HeaderXSLT
                    try {
                        Message.Soap_HeaderRequest.append(
                                ConvXMLuseXSLT(messageQueueVO.getQueue_Id(), MessageUtils.MakeEntryOutHeader(messageQueueVO, MsgDirectionVO_Key), // стандартный заголовок c учетом системы-получателя
                                        Message.MessageTemplate4Perform.getHeaderXSLT(),  // через HeaderXSLT
                                        Message.MsgReason, MessegeSend_Log,
                                        Message.MessageTemplate4Perform.getIsDebugged()
                                )
                                        .substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
                        );
                    } catch ( TransformerException exception ) {
                        MessegeSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь заголовка:{" + Message.MessageTemplate4Perform.getHeaderXSLT() +"}");

                        theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                                "Header XSLT fault: " + ConvXMLuseXSLTerr  + " for " + Message.MessageTemplate4Perform.getHeaderXSLT(), 1244,
                                messageQueueVO.getRetry_Count(), MessegeSend_Log);

                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, MessageUtils.MakeEntryOutHeader(messageQueueVO, MsgDirectionVO_Key),
                        //        "Header XSLT fault: " + ConvXMLuseXSLTerr  + " for " + Message.MessageTemplate4Perform.getHeaderXSLT(),  monitoringQueueVO, MessegeSend_Log);
                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                        return -5L;
                    }
                    else Message.Soap_HeaderRequest.append( MessageUtils.MakeEntryOutHeader( messageQueueVO, MsgDirectionVO_Key) );
                    // Собсвенно, ВЫЗОВ!
                    Function_Result = MessageHttpSend.sendSoapMessage( messageQueueVO, Message, theadDataAccess, monitoringQueueVO, MessegeSend_Log);
                    // MessegeSend_Log.info("sendSOAPMessage:" + Queue_Direction + " [" + Queue_Id + "] для SOAP=:\n" + Message.XML_MsgSEND);
                }
                if ( Function_Result <0 ) {
                    // TODO
                    // Надо бы всзести переменную - что c Http всё плохо, но пост-обработчик надо всё же вызвать хоть раз.
                     AnswXSLTQueue_Direction = messageQueueVO.getQueue_Direction();
                    break;
                }

                // шаблон MsgAnswXSLT заполнен
                if ( Message.MessageTemplate4Perform.getMsgAnswXSLT() != null) {
                    if ( Message.MessageTemplate4Perform.getIsDebugged() == true ) {
                        MessegeSend_Log.info(Queue_Direction + " [" + Queue_Id + "] MsgAnswXSLT:\n" + Message.MessageTemplate4Perform.getMsgAnswXSLT() );
                    }
                    try {
                    Message.XML_MsgRESOUT.append(
                            ConvXMLuseXSLT(
                                    Queue_Id, Message.XML_ClearBodyResponse.toString(), // очищенный от ns: /Envelope/Body
                                    Message.MessageTemplate4Perform.getMsgAnswXSLT(),  // через MsgAnswXSLT
                                    Message.MsgReason, MessegeSend_Log,
                                    Message.MessageTemplate4Perform.getIsDebugged()
                                    )
                                    .substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
                    );
                    } catch ( Exception exception ) {
                        MessegeSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь ответа:{" + Message.MessageTemplate4Perform.getMsgAnswXSLT() +"}");

                        theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO, //.getQueue_Id(),
                                "Answer XSLT fault: " + ConvXMLuseXSLTerr  + " on " + Message.MessageTemplate4Perform.getMsgAnswXSLT(), 1243,
                                messageQueueVO.getRetry_Count(), MessegeSend_Log);

                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO,Message.XML_ClearBodyResponse.toString(),
                        //        "Answer XSLT fault: " + ConvXMLuseXSLTerr  + " on " + Message.MessageTemplate4Perform.getMsgAnswXSLT(),  monitoringQueueVO, MessegeSend_Log);
                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                        return -501L;
                    }
                    // MessegeSend_Log.info(Queue_Direction +" ["+ Queue_Id +"] Message.MessageTemplate4Perform.getIsDebugged()=" + Message.MessageTemplate4Perform.getIsDebugged() );
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessegeSend_Log.info(Queue_Direction +" ["+ Queue_Id +"] преобразовали XML-ответ в:\n" + Message.XML_MsgRESOUT.toString() );
                }
                else // берем как есть без преобразования
                {
                    Message.XML_MsgRESOUT.append(Message.XML_ClearBodyResponse.toString());
                    if ( Message.MessageTemplate4Perform.getIsDebugged()  == true )
                    MessegeSend_Log.info(Queue_Direction + " [" + Queue_Id + "] используем XML-ответ как есть без преобразования:(" + Message.XML_MsgRESOUT.toString() + ")");
                }
                    // Проверяем наличие TagNext ="Next" в XML_MsgRESOUT
                AnswXSLTQueue_Direction = MessageUtils.PrepareConfirmation(  theadDataAccess,  messageQueueVO,  Message, MessegeSend_Log );
                messageQueueVO.setQueue_Direction(AnswXSLTQueue_Direction);

                if ( !AnswXSLTQueue_Direction.equals(XMLchars.DirectRESOUT))
                    // TODO Надо бы всзести переменную - что c XSLT всё плохо, но пост-обработчик надо всё же вызвать хоть раз.
                { theadDataAccess.do_SelectMESSAGE_QUEUE(  messageQueueVO, MessegeSend_Log );
                    break;
                }

                messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
                messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
                messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());

                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_ClearBodyResponse.toString(), Message.XML_MsgRESOUT.toString(),  monitoringQueueVO, MessegeSend_Log);
                ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);

                // получение и преобразование результатов
            case XMLchars.DirectRESOUT : //"RESOUT"
                // проверяем НАЛИЧИЕ пост-обработчика в Шаблоне
                if ( Message.MessageTemplate4Perform.getConfigPostExec() != null ) { // 1) ConfigPostExec
                    if ( !Queue_Direction.equals("SEND") ) {
                        // надо читать из БД
                        MessegeSend_Log.error(Queue_Direction +"-> DELOUT/ATTOUT/ERROUT ["+ Queue_Id +"] читаем SEND БД тело XML" );
                        MessageUtils.ReadMessage( theadDataAccess, Queue_Id, Message, MessegeSend_Log);
                        Message.XML_MsgSEND = Message.XML_MsgOUT.toString();
                        Queue_Direction = XMLchars.DirectPOSTOUT;
                        MessegeSend_Log.error("["+ Queue_Id +"] Этот код для повторнй обработки Ответв на Исходяе событие ещё не написан.  " );
                        theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                "Этот код для повторнй обработки Ответв на Исходяе событие ещё не написан. Сделано от защиты зацикливания", 1232,
                                messageQueueVO.getRetry_Count(),  MessegeSend_Log);
                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_MsgSEND,
                        //        "Этот код для повторнй обработки Ответв на Исходяе событие ещё не написан. Сделано от защиты зацикливания",  monitoringQueueVO, MessegeSend_Log);
                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                        return -11L;
                    }
                    // TODO Надо бы всзести переменную - что  пост-обработчик  всё же вызвался хоть раз и если ошибка, то больше не надо.
                    messageQueueVO.setQueue_Direction(XMLchars.DirectRESOUT);
                    if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.JavaClassExeMetod) )
                    { // 2.1) Это JDBC-обработчик
                        if ( Message.MessageTemplate4Perform.getEnvelopeXSLTPost() != null ) { // 2) EnvelopeXSLTPost
                            if ( Message.MessageTemplate4Perform.getEnvelopeXSLTPost().length() > 0 ) {
                                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                MessegeSend_Log.info("["+ Queue_Id +"] Шаблон EnvelopeXSLTPost для пост-обработки(" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost() + ")");
                                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                MessegeSend_Log.info("["+ Queue_Id +"] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO,  Message, MessegeSend_Log) );

                                String Passed_Envelope4XSLTPost;
                                try {
                                    Passed_Envelope4XSLTPost= ConvXMLuseXSLT(messageQueueVO.getQueue_Id(),
                                            MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO, Message, MessegeSend_Log), // Искуственный Envelope/Head/Body is XML_MsgRESOUT
                                            Message.MessageTemplate4Perform.getEnvelopeXSLTPost(),  // через EnvelopeXSLTPost
                                            Message.MsgReason, MessegeSend_Log, Message.MessageTemplate4Perform.getIsDebugged());
                                } catch ( TransformerException exception ) {
                                    MessegeSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-пост-преобразователь ответа:{" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost() +"}");
                                    theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                            "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost(), 1235,
                                            messageQueueVO.getRetry_Count(),  MessegeSend_Log);
                                    ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO,  Message, MessegeSend_Log),
                                    //        "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost(),  monitoringQueueVO, MessegeSend_Log);
                                    return -101L;
                                }
                                if ( Passed_Envelope4XSLTPost.equals(XMLchars.EmptyXSLT_Result))
                                {   MessegeSend_Log.error("["+ Queue_Id +"] Шаблон для пост-обработки(" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost() + ")");
                                    MessegeSend_Log.error("["+ Queue_Id +"] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4XSLTPost(  messageQueueVO,  Message, MessegeSend_Log) );
                                    MessegeSend_Log.error("["+ Queue_Id +"] Ошибка преобразования XSLT для пост-обработки " + Message.MsgReason.toString() );
                                    theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                            "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(), 1232,
                                            messageQueueVO.getRetry_Count(),  MessegeSend_Log);
                                    ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Passed_Envelope4XSLTPost,
                                    //        "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(),  monitoringQueueVO, MessegeSend_Log);
                                    return -12L;

                                }

                                final int resultSQL = XmlSQLStatement.ExecuteSQLincludedXML( theadDataAccess, Passed_Envelope4XSLTPost, messageQueueVO, Message, MessegeSend_Log);
                                if (resultSQL != 0) {
                                    MessegeSend_Log.error("["+ Queue_Id +"] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO,  Message, MessegeSend_Log) );
                                    MessegeSend_Log.error("["+ Queue_Id +"] Ошибка ExecuteSQLinXML:" + Message.MsgReason.toString() );
                                    theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                            "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), 1232,
                                            messageQueueVO.getRetry_Count(),  MessegeSend_Log);
                                    ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Passed_Envelope4XSLTPost,
                                    //        "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(),  monitoringQueueVO, MessegeSend_Log);
                                    return -13L;
                                }
                                else
                                {if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                    MessegeSend_Log.info("["+ Queue_Id +"] Исполнение ExecuteSQLinXML:" + Message.MsgReason.toString() );
                                    ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                                    /*if ( theadDataAccess.do_SelectMESSAGE_QUEUE(  messageQueueVO, MessegeSend_Log ) == 0 )
                                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4XSLTPost,
                                                "ExecuteSQLinXML: " + Message.MsgReason.toString(),  monitoringQueueVO, MessegeSend_Log);
                                    else
                                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4XSLTPost,
                                                "do_SelectMESSAGE_QUEUE fault " ,  monitoringQueueVO, MessegeSend_Log);
                                     */
                                }
                            }
                            else
                            {   // Нет EnvelopeXSLTPost - надо орать! прописан Java класс, а EnvelopeXSLTPost нет
                                MessegeSend_Log.error("["+ Queue_Id +"] В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost");
                                theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                        "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost", 1232,
                                        messageQueueVO.getRetry_Count(),  MessegeSend_Log);
                                return -14L;
                            }
                        }
                        else
                        {
                            // Нет EnvelopeXSLTPost - надо орать!
                            MessegeSend_Log.error("["+ Queue_Id +"] В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost");
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost", 1232,
                                    messageQueueVO.getRetry_Count(),  MessegeSend_Log);
                            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.MessageTemplate4Perform.getPropExeMetodPostExec(),
                            //        "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost",  monitoringQueueVO, MessegeSend_Log);
                            return -15L;
                        }
                    }
                    if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.WebRestExeMetod) )
                    { // 2.2) Это Rest-HttpGet-вызов

                        if (( Message.MessageTemplate4Perform.getPropHostPostExec() == null ) ||
                                ( Message.MessageTemplate4Perform.getPropUserPostExec() == null ) ||
                        ( Message.MessageTemplate4Perform.getPropPswdPostExec() == null ) ||
                        ( Message.MessageTemplate4Perform.getPropUrlPostExec()  == null ) )
                        {
                            // Нет параметров для Rest-HttpGet - надо орать!
                            MessegeSend_Log.error("["+ Queue_Id +"] В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet включая логин/пароль");
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet включая логин/пароль", 1232,
                                    messageQueueVO.getRetry_Count(),  MessegeSend_Log);
                            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.MessageTemplate4Perform.getPropExeMetodPostExec(),
                            //        "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet включая логин/пароль",  monitoringQueueVO, MessegeSend_Log);
                            return -16L;
                        }
                        String EndPointUrl= null;
                        try {

                            if ( StringUtils.substring(Message.MessageTemplate4Perform.getPropHostPostExec(),0,"http".length()).equalsIgnoreCase("http") )
                                EndPointUrl =
                            Message.MessageTemplate4Perform.getPropHostPostExec() +
                                    Message.MessageTemplate4Perform.getPropUrlPostExec();
                            else
                                EndPointUrl = "http://" + Message.MessageTemplate4Perform.getPropHostPostExec() +
                                        Message.MessageTemplate4Perform.getPropUrlPostExec();
                            // Ставим своенго клиента ! ?
                            Unirest.setHttpClient( Message.RestHermesAPIHttpClient);
                            String RestResponse =
                            Unirest.get(EndPointUrl)
                                    .queryString("queue_id", Queue_Id.toString())
                                    .basicAuth(Message.MessageTemplate4Perform.getPropUserPostExec(),
                                                Message.MessageTemplate4Perform.getPropPswdPostExec())
                                    .asString().getBody();
                            if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                MessegeSend_Log.info("[" + messageQueueVO.getQueue_Id() + "]" +"MetodPostExec.Unirest.get(" + EndPointUrl + ") RestResponse=(" + RestResponse + ")");
                            theadDataAccess.do_SelectMESSAGE_QUEUE(  messageQueueVO, MessegeSend_Log );
                            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                            /*if ( theadDataAccess.do_SelectMESSAGE_QUEUE(  messageQueueVO, MessegeSend_Log ) == 0 )
                                ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, EndPointUrl + "?queue_id=" + Queue_Id.toString(),
                                        RestResponse,  monitoringQueueVO, MessegeSend_Log);
                            else
                                ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, EndPointUrl + "?queue_id=" + Queue_Id.toString(),
                                        RestResponse,  monitoringQueueVO, MessegeSend_Log);
                            */

                        } catch ( UnirestException e) {
                            // возмущаемся, но оставляем сообщение в ResOUT что бы обработчик в кроне мог доработать
                            MessegeSend_Log.error("["+ Queue_Id +"] Ошибка пост-обработки HttpGet(" + EndPointUrl + "):" + e.toString() );
                            theadDataAccess.doUPDATE_MessageQueue_SetMsg_Reason(messageQueueVO,
                                    "Ошибка пост-обработки HttpGet(" + EndPointUrl + "):" + sStackTracе.strInterruptedException(e), 123567,
                                    messageQueueVO.getRetry_Count(),  MessegeSend_Log);
                            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, EndPointUrl + "?queue_id=" + Queue_Id.toString(),
                            //        "Ошибка пост-обработки HttpGet(" + EndPointUrl + "):" + sStackTracе.strInterruptedException(e),  monitoringQueueVO, MessegeSend_Log);
                            return -17L;
                        }
                    }

                }
                else
                {if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessegeSend_Log.info("["+ Queue_Id +"] ExeMetod для пост-обработки(" + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + ")");
                }
                // вызов пост-обработчика завершён

            case "ERROUT":
                // вызов пост-обработчика ??? - вызов при необходимости, ноавая фича

            case "DELOUT":
                break;
          default:
                break;
        }

        if ( Message.MessageTemplate4Perform.getIsDebugged() ) {
            MessegeSend_Log.info("[" + Queue_Id + "] string 664:" );
            MessegeSend_Log.info("[" + Queue_Id + "] AnswXSLTQueue_Direction='" + AnswXSLTQueue_Direction + "'");
            MessegeSend_Log.info("[" + Queue_Id + "] messageQueueVO.getQueue_Direction()='" + messageQueueVO.getQueue_Direction() + "'");
        }

        if ( AnswXSLTQueue_Direction.equals(XMLchars.DirectERROUT)
        && !messageQueueVO.getQueue_Direction().equals(XMLchars.DirectRESOUT)) {
            if ( Message.MessageTemplate4Perform.getIsDebugged() )
                MessegeSend_Log.info("["+ Queue_Id +"] ExeMetod для пост-обработки сообщения, получившего ошибку от внешней системы (" + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + ")");
            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec() != null ) // если  пост-обработчик вообще указан !
            // вызов пост-обработчика ??? - вызов при необходимости, ноавая фича
            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.WebRestExeMetod) )
            { // 2.2) Это Rest-HttpGet-вызов
                ;
                if (( Message.MessageTemplate4Perform.getPropHostPostExec() == null ) ||
                        ( Message.MessageTemplate4Perform.getPropUserPostExec() == null ) ||
                        ( Message.MessageTemplate4Perform.getPropPswdPostExec() == null ) ||
                        ( Message.MessageTemplate4Perform.getPropUrlPostExec()  == null ) )
                {
                    // Нет параметров для Rest-HttpGet - надо орать!
                    MessegeSend_Log.error("["+ Queue_Id +"] В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet вклюая логин/пароль");
                    theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                            "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet вклюая логин/пароль", 1232,
                            messageQueueVO.getRetry_Count(),  MessegeSend_Log);
                    ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.MessageTemplate4Perform.getPropExeMetodPostExec(),
                    //        "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet вклюая логин/пароль",  monitoringQueueVO, MessegeSend_Log);
                    return -161L;
                }
                String EndPointUrl= null;
                try {

                    if ( StringUtils.substring(Message.MessageTemplate4Perform.getPropHostPostExec(),0,"http".length()).equalsIgnoreCase("http") )
                        EndPointUrl =
                                Message.MessageTemplate4Perform.getPropHostPostExec() +
                                        Message.MessageTemplate4Perform.getPropUrlPostExec();
                    else
                        EndPointUrl = "http://" + Message.MessageTemplate4Perform.getPropHostPostExec() +
                                Message.MessageTemplate4Perform.getPropUrlPostExec();
                    // Ставим своенго клиента ! ?
                    Unirest.setHttpClient( Message.RestHermesAPIHttpClient);
                    //String RestResponse =
                            Unirest.get(EndPointUrl)
                                    .queryString("queue_id", Queue_Id.toString())
                                    .basicAuth(Message.MessageTemplate4Perform.getPropUserPostExec(),
                                            Message.MessageTemplate4Perform.getPropPswdPostExec())
                                    .asString().getBody();
                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, EndPointUrl + "?queue_id=" + Queue_Id.toString(),
                    //        RestResponse,  monitoringQueueVO, MessegeSend_Log);
                    ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);

                } catch ( UnirestException e) {
                    // возмущаемся, но оставляем сообщение в ResOUT что бы обработчик в кроне мог доработать
                    MessegeSend_Log.error("["+ Queue_Id +"] Ошибка пост-обработки HttpGet(" + EndPointUrl + "):" + e.toString() );
                    theadDataAccess.doUPDATE_MessageQueue_SetMsg_Reason(messageQueueVO,
                            "Ошибка пост-обработки HttpGet(" + EndPointUrl + "):" + sStackTracе.strInterruptedException(e), 135699,
                            messageQueueVO.getRetry_Count(),  MessegeSend_Log);
                    ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, EndPointUrl + "?queue_id=" + Queue_Id.toString(),
                    //        "Ошибка пост-обработки HttpGet(" + EndPointUrl + "):" + sStackTracе.strInterruptedException(e),  monitoringQueueVO, MessegeSend_Log);
                    return -171L;
                }
            }
            //---------------------
            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec() != null ) // если  пост-обработчик вообще указан !
            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.JavaClassExeMetod) )
            {
                if (Message.MessageTemplate4Perform.getErrTransXSLT() != null) { // 2) getErrTransXSLT
                    if (Message.MessageTemplate4Perform.getErrTransXSLT().length() > 0) {
                        if (Message.MessageTemplate4Perform.getIsDebugged())
                            MessegeSend_Log.info("[" + Queue_Id + "] Шаблон ErrTransXSLT для пост-обработки(" + Message.MessageTemplate4Perform.getErrTransXSLT() + ")");
                        if (Message.MessageTemplate4Perform.getIsDebugged())
                            MessegeSend_Log.info("[" + Queue_Id + "] ErrTransXSLT:" + MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessegeSend_Log));

                        String Passed_Envelope4ErrTransXSLT;
                        try {
                            Passed_Envelope4ErrTransXSLT = ConvXMLuseXSLT(messageQueueVO.getQueue_Id(),
                                    MessageUtils.PrepareEnvelope4ErrTransXSLT( messageQueueVO, Message, MessegeSend_Log), // Искуственный Envelope/Head/Body is XML_MsgRESOUT
                                    Message.MessageTemplate4Perform.getErrTransXSLT(),  // через getErrTransXSLT
                                    Message.MsgReason, MessegeSend_Log, Message.MessageTemplate4Perform.getIsDebugged());
                        } catch (TransformerException exception) {
                            MessegeSend_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT для обработки ERROUT ответа:{" + Message.MessageTemplate4Perform.getErrTransXSLT() + "}");
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "Ошибка преобразования XSLT для обработки ERROUT" + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getErrTransXSLT(), 1295,
                                    messageQueueVO.getRetry_Count(), MessegeSend_Log);
                            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessegeSend_Log),
                            //        "Ошибка преобразования XSLT для обработки ERROUT " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getErrTransXSLT(), monitoringQueueVO, MessegeSend_Log);
                            return -18L;
                        }
                        if (Passed_Envelope4ErrTransXSLT.equals(XMLchars.EmptyXSLT_Result)) {
                            MessegeSend_Log.error("[" + Queue_Id + "] Шаблон для обработки ERROUT(" + Message.MessageTemplate4Perform.getErrTransXSLT() + ")");
                            MessegeSend_Log.error("[" + Queue_Id + "] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessegeSend_Log));
                            MessegeSend_Log.error("[" + Queue_Id + "] Ошибка преобразования XSLT для обработки ERROUT " + Message.MsgReason.toString());
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "Ошибка преобразования XSLT для обработки ERROUT " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(), 1292,
                                    messageQueueVO.getRetry_Count(), MessegeSend_Log);
                            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, Passed_Envelope4ErrTransXSLT,
                            //        "Ошибка преобразования XSLT для обработки ERROUT " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(), monitoringQueueVO, MessegeSend_Log);
                            return -19L;

                        }

                        final int resultSQL = XmlSQLStatement.ExecuteSQLincludedXML(theadDataAccess, Passed_Envelope4ErrTransXSLT, messageQueueVO, Message, MessegeSend_Log);
                        if (resultSQL != 0) {
                            MessegeSend_Log.error("[" + Queue_Id + "] Envelope4XSLTPost:" + MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessegeSend_Log));
                            MessegeSend_Log.error("[" + Queue_Id + "] Ошибка ExecuteSQLinXML:" + Message.MsgReason.toString());
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), 1292,
                                    messageQueueVO.getRetry_Count(), MessegeSend_Log);
                            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, Passed_Envelope4ErrTransXSLT,
                            //        "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), monitoringQueueVO, MessegeSend_Log);
                            return -20L;
                        } else {
                            if (Message.MessageTemplate4Perform.getIsDebugged())
                                MessegeSend_Log.info("[" + Queue_Id + "] Исполнение ExecuteSQLinXML:" + Message.MsgReason.toString());
                            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessegeSend_Log);
                            /*if (theadDataAccess.do_SelectMESSAGE_QUEUE(messageQueueVO, MessegeSend_Log) == 0)
                                ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4ErrTransXSLT,
                                        "ExecuteSQLinXML: " + Message.MsgReason.toString(), monitoringQueueVO, MessegeSend_Log);
                            else
                                ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4ErrTransXSLT,
                                        "do_SelectMESSAGE_QUEUE fault ", monitoringQueueVO, MessegeSend_Log);
                            */
                        }
                    } else {   // Нет EnvelopeXSLTPost - надо орать! прописан Java класс, а EnvelopeXSLTPost нет
                        MessegeSend_Log.error("[" + Queue_Id + "] В шаблоне для обработки ERROUT " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет ErrTransXSLT");
                        theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет ErrTransXSLT", 1292,
                                messageQueueVO.getRetry_Count(), MessegeSend_Log);
                        return -21L;
                    }
                }
            }
            //-----------------------
        }

/*
        try {  SimpleHttpClient.close(); } catch ( IOException e) {
           MessegeSend_Log.error("под конец  ошибка SimpleHttpClient.close(): " + e.getMessage() );
            Message.SimpleHttpClient = null;
            return messageQueueVO.getQueue_Id();      }
*/

        return messageQueueVO.getQueue_Id();
    }

    public CloseableHttpClient getCloseableHttpClient( SSLContext sslContext, MessageTemplate4Perform messageTemplate4Perform,
                                                                                               Logger getHttpClient_Log) {
        Integer SocketTimeout =  messageTemplate4Perform.getPropTimeout_Read() * 1000;
        Integer ConnectTimeout = messageTemplate4Perform.getPropTimeout_Conn() * 1000;

        if ( this.client == null ) {
            this.client = new DefaultHttpClient( this.ExternalConnectionManager );
        }
        HttpParams httpParameters = new BasicHttpParams();

        HttpConnectionParams.setConnectionTimeout(httpParameters, ConnectTimeout) ; // connectTimeoutInMillis);
        HttpConnectionParams.setSoTimeout(httpParameters, SocketTimeout );  //readTimeoutInMillis;
        HttpHost proxyHost;
        String isProxySet = System.getProperty("http.proxySet");
        getHttpClient_Log.info("getCloseableHttpClient(): System.getProperty(\"http.proxySet\") [" + isProxySet + "]");
        if ( (isProxySet != null) && (isProxySet.equals("true")) ) {
            getHttpClient_Log.info("getCloseableHttpClient(): System.getProperty(\"http.proxyHost\") [" + System.getProperty("http.proxyHost") + "]");
            getHttpClient_Log.info("getCloseableHttpClient(): System.getProperty(\"http.proxyPort\") [" + System.getProperty("http.proxyPort") + "]");
            proxyHost = new HttpHost( System.getProperty("http.proxyHost"), Integer.parseInt(System.getProperty("http.proxyPort")) );
            ConnRouteParams.setDefaultProxy(httpParameters, proxyHost );
            getHttpClient_Log.info("ConnRouteParams.DEFAULT_PROXY[" + ConnRouteParams.DEFAULT_PROXY + "]");
        }

        this.client.setParams(httpParameters);
        String EndPointUrl;

        if ( StringUtils.substring(messageTemplate4Perform.getEndPointUrl(),0,"http".length()).equalsIgnoreCase("http") )
            EndPointUrl = messageTemplate4Perform.getEndPointUrl();
        else
            EndPointUrl = "http://" + messageTemplate4Perform.getEndPointUrl();
        if ( StringUtils.substring(EndPointUrl,0,"https".length()).equalsIgnoreCase("https") ) {
            SSLSocketFactory factory;
            int port;
            endpointProperties = Security.builder().build();
            URI Uri_4_Request;
            try {
                try {
                    Uri_4_Request = new URI(EndPointUrl);

                } catch (URISyntaxException ex) {
                    throw new SoapClientException(String.format("URI [%s] is malformed", EndPointUrl) + ex.getMessage(), ex);
                }
                factory = SSLUtils.getFactory(endpointProperties);
                port = Uri_4_Request.getPort();
                registerTlsScheme(factory, port);

            } catch (GeneralSecurityException ex) {
                throw new SoapClientException(ex);
            }
        }
        return  this.client;

    }
    //private URI endpointUri;


    public CloseableHttpClient getProxedHttpClient( SSLContext sslContext, MessageTemplate4Perform messageTemplate4Perform,
                                                       Logger getHttpClient_Log) {
        Integer SocketTimeout =  messageTemplate4Perform.getPropTimeout_Read() * 1000;
        Integer ConnectTimeout = messageTemplate4Perform.getPropTimeout_Conn() * 1000;
        CloseableHttpClient  safeHttpClient=null;
        if ( this.httpClient != null )
            try {  this.httpClient.close();
                   this.httpClient = null;
            } catch ( IOException e) {
                getHttpClient_Log.error("под конец  ошибка httpClient.close(): " + e.getMessage() );
                this.httpClient = null;
                return null;     }


        if ( this.httpClient == null ) {
            RequestConfig rc;
            HttpHost proxyHost;
            String isProxySet = System.getProperty("http.proxySet");
            getHttpClient_Log.info("getProxedHttpClient(): System.getProperty(\"http.proxySet\") [" + isProxySet + "]");
            // if ( (isProxySet != null) && (isProxySet.equals("true")) ) {
                getHttpClient_Log.info("getProxedHttpClient(): System.getProperty(\"http.proxyHost\") [" + System.getProperty("http.proxyHost") + "]");
                getHttpClient_Log.info("getProxedHttpClient(): System.getProperty(\"http.proxyPort\") [" + System.getProperty("http.proxyPort") + "]");
                proxyHost = new HttpHost( System.getProperty("http.proxyHost"), Integer.parseInt(System.getProperty("http.proxyPort")) );

            // }
        /*
        try {
            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustSelfSignedStrategy() {
                public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    return true;
                }
            }).build();
*/
            rc = RequestConfig.custom()
                    .setConnectionRequestTimeout(ConnectTimeout)
                    .setConnectTimeout(ConnectTimeout)
                    .setSocketTimeout(SocketTimeout)
                    .build();
            //threadSafeClientConnManager = new HttpClientConnectionManager();
            //threadSafeClientConnManager.setMaxTotal((Integer) 99);
            //threadSafeClientConnManager.setDefaultMaxPerRoute((Integer) 99);
            CredentialsProvider credentialsPovider = new BasicCredentialsProvider();
            credentialsPovider.setCredentials(new AuthScope(System.getProperty("http.proxyHost"), Integer.parseInt(System.getProperty("http.proxyPort")) ), new
                    UsernamePasswordCredentials(System.getProperty("http.proxyUser"), System.getProperty("http.proxyPassword")));

            HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                    .disableDefaultUserAgent()
                    .disableRedirectHandling()
                    .disableAutomaticRetries()
                    .setUserAgent("Mozilla/5.0")
                    .setSSLContext(sslContext)
                    .disableAuthCaching()
                    .disableConnectionState()
                    .disableCookieManagement()
                    .setProxy(proxyHost)
                    .setDefaultCredentialsProvider(credentialsPovider)
                    .useSystemProperties() // HE-5663  https://stackoverflow.com/questions/5165126/without-changing-code-how-to-force-httpclient-to-use-proxy-by-environment-varia
                    //      .setConnectionManager( threadSafeClientConnManager )
                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
                    .setConnectionTimeToLive(SocketTimeout + 5, TimeUnit.SECONDS)
                    .evictIdleConnections((long) (SocketTimeout + 5) * 2, TimeUnit.SECONDS);

            httpClientBuilder.setDefaultRequestConfig(rc);
            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
            cm.setMaxTotal(50 * 2);
            cm.setDefaultMaxPerRoute(2);
            cm.setValidateAfterInactivity(1200 * 1000);
            httpClientBuilder.setConnectionManager(cm);

            safeHttpClient = httpClientBuilder.build();

            this.httpClient = safeHttpClient;

/****************************************
 PoolingHttpClientConnectionManager syncConnectionManager = new PoolingHttpClientConnectionManager();
 syncConnectionManager.setMaxTotal((Integer)4);
 syncConnectionManager.setDefaultMaxPerRoute((Integer)2);

 httpClientBuilder = HttpClientBuilder.create()
 .disableDefaultUserAgent()
 .disableRedirectHandling()
 .disableAutomaticRetries()
 .setUserAgent("Mozilla/5.0")
 .setSSLContext(sslContext)
 .disableAuthCaching()
 .disableConnectionState()
 .disableCookieManagement()
 .setConnectionManager(syncConnectionManager)
 .setSSLHostnameVerifier(new NoopHostnameVerifier());

 httpClientBuilder.setConnectionTimeToLive( SocketTimeout + ConnectTimeout, TimeUnit.SECONDS);
 httpClientBuilder.evictIdleConnections((long) (SocketTimeout + ConnectTimeout)*2, TimeUnit.SECONDS);

 //            PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager(r.build());
 //            cm.setMaxTotal(maxConnect * 2);
 //            cm.setDefaultMaxPerRoute(2);
 //            cm.setValidateAfterInactivity(timeout * 1000);
 //            builder.setConnectionManager(cm);

 RequestConfig rc = RequestConfig.custom()
 .setConnectionRequestTimeout(ConnectTimeout * 1000)
 .setConnectTimeout(ConnectTimeout * 1000)
 .setSocketTimeout( SocketTimeout * 1000)
 .build();
 httpClientBuilder.setDefaultRequestConfig(rc);
 ///HttpClients.custom().build();
 HostnameVerifier allowAllHosts = new NoopHostnameVerifier();
 SSLConnectionSocketFactory connectionFactory = new SSLConnectionSocketFactory(sslContext, allowAllHosts);
 getHttpClient_Log.info("httpClientBuilder: setSSLHostnameVerifier(allowAllHosts)");

 unsafeHttpClient = httpClientBuilder
 .setSSLHostnameVerifier(allowAllHosts)
 .setSSLSocketFactory(connectionFactory)
 .build();

 //                    .setSSLContext(sslContext)
 //                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
 //                    .build();
 //
 //            unsafeHttpClient = HttpClients.custom().setSSLContext(sslContext)
 //                    .setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
 /* ПРИМЕР !!!
 RequestConfig clientConfig = RequestConfig.custom()
 .setConnectTimeout(((Long)connectionTimeout).intValue())
 .setSocketTimeout(((Long)socketTimeout).intValue())
 .setConnectionRequestTimeout(((Long)socketTimeout).intValue())
 .setProxy(proxy)
 .build();
 PoolingHttpClientConnectionManager syncConnectionManager = new PoolingHttpClientConnectionManager();
 syncConnectionManager.setMaxTotal((Integer)maxTotal);
 syncConnectionManager.setDefaultMaxPerRoute((Integer)maxPerRoute);
 setOption(Option.HTTPCLIENT,
 HttpClientBuilder.create().setDefaultRequestConfig(clientConfig).setConnectionManager(syncConnectionManager).build());
 */
/*
        if ( client == null ) {

            client = new DefaultHttpClient( this.ExternalConnectionManager );
        }

        */
        }
        return safeHttpClient;

    }
    private Security endpointProperties;

    private void registerTlsScheme(SchemeLayeredSocketFactory factory, int port) {
        Scheme sch = new Scheme(HTTPS, port, factory);
        client.getConnectionManager().getSchemeRegistry().register(sch);
    }

    private  boolean TestXMLByXSD(@NotNull String xmldata, @NotNull String xsddata, StringBuilder MsgResult,  Logger MessegeSend_Log)// throws Exception
    {
        Validator valid=null;
        StreamSource reqwsdl=null, xsdss = null;
        Schema shm= null;

        try
        { reqwsdl = new StreamSource(new ByteArrayInputStream(xmldata.getBytes()));
            xsdss   = new StreamSource(new ByteArrayInputStream(xsddata.getBytes()));
            shm = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(xsdss);
            valid =shm.newValidator();
            valid.validate(reqwsdl);

        }
        catch ( Exception exp ) {
            MessegeSend_Log.error("Exception: " + exp.getMessage());
            MsgResult.setLength(0);
            MsgResult.append( "TestXMLByXSD:"  + strInterruptedException(exp) );
            return false;}
        MessegeSend_Log.info("validateXMLSchema message\n" + xmldata + "\n is VALID for XSD\n" + xsddata );
        return true;
    }


    private   String ConvXMLuseXSLT(@NotNull Long QueueId, @NotNull String xmldata, @NotNull String XSLTdata, StringBuilder MsgResult, Logger MessegeSend_Log, boolean IsDebugged )
            throws TransformerException
    { StreamSource source,srcxslt;
        Transformer transformer;
        StreamResult result;
        ByteArrayInputStream xmlInputStream=null;
        ByteArrayOutputStream fout=new ByteArrayOutputStream();
        String res=XMLchars.EmptyXSLT_Result;
        ConvXMLuseXSLTerr="";
        try {
            xmlInputStream  = new ByteArrayInputStream(xmldata.getBytes("UTF-8"));

        }
        catch ( Exception exp ) {
            ConvXMLuseXSLTerr = strInterruptedException(exp);
            exp.printStackTrace();
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.ByteArrayInputStream Exception" );
            MessegeSend_Log.error("["+ QueueId  + "] Exception: " + ConvXMLuseXSLTerr );
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT:"  + ConvXMLuseXSLTerr );
            return XMLchars.EmptyXSLT_Result ;
        }

        source = new StreamSource(xmlInputStream);
        try {
            srcxslt = new StreamSource(new ByteArrayInputStream(XSLTdata.getBytes("UTF-8")));
        }
                catch ( Exception exp ) {
                ConvXMLuseXSLTerr = strInterruptedException(exp);
                exp.printStackTrace();
                System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.ByteArrayInputStream Exception" );
                MessegeSend_Log.error("["+ QueueId  + "] Exception: " + ConvXMLuseXSLTerr );
                MsgResult.setLength(0);
                MsgResult.append( "ConvXMLuseXSLT:"  + ConvXMLuseXSLTerr );
                return XMLchars.EmptyXSLT_Result ;
            }
        result = new StreamResult(fout);
        try
        {
            TransformerFactory XSLTransformerFactory = TransformerFactory.newInstance();
             XSLTransformerFactory.setErrorListener( XSLTErrorListener ); //!!!! java.lang.IllegalArgumentException: ErrorListener !!!
           /* XSLTransformerFactory.setErrorListener(new ErrorListener() {
                public void warning(TransformerException te) {
                    log.warn("Warning received while processing a stylesheet", te);
                }
                */
         // transformer = TransformerFactory.newInstance().newTransformer(srcxslt);
            transformer = XSLTransformerFactory.newTransformer(srcxslt);
            if ( transformer != null) {
                transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
                transformer.transform(source, result);
            }
            else result = null;

            if ( result != null) {
                res = fout.toString();
                // System.err.println("result != null, res:" + res );
                if ( res.length() < XMLchars.EmptyXSLT_Result.length())
                    res = XMLchars.EmptyXSLT_Result;
            }
            else {
                res = XMLchars.EmptyXSLT_Result;
                // System.err.println("result= null, res:" + res );
            }
        try { fout.close();} catch( IOException IOexc)  { ; }
                if ( IsDebugged ) {
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML IN ): " + xmldata);
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML out ): " + res);
            }
        }
        catch ( TransformerException exp ) {
            ConvXMLuseXSLTerr = strInterruptedException(exp);
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.Transformer Exception" );
            exp.printStackTrace();

            if (  !IsDebugged ) {
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XML IN ): " + xmldata);
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XML out ): " + res);
            }
            MessegeSend_Log.error("["+ QueueId  + "] Transformer.Exception: " + ConvXMLuseXSLTerr);
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT.Transformer:"  + ConvXMLuseXSLTerr );
            throw exp;
            // return XMLchars.EmptyXSLT_Result ;
        }
        return(res);
    }

}
