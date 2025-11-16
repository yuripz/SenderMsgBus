package net.plumbing.msgbus.threads;


import net.plumbing.msgbus.common.json.XML;
import net.sf.saxon.s9api.*;

import net.sf.saxon.serialize.SerializationProperties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import javax.xml.XMLConstants;
//import javax.xml.transform.*;
//import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.*;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.model.*;
import net.plumbing.msgbus.threads.utils.*;
import net.plumbing.msgbus.common.XMLchars;
//import net.plumbing.msgbus.common.xlstErrorListener;

// import static net.plumbing.msgbus.common.sStackTrace.strInterruptedException;

public class PerformQueueMessages {

    // private xlstErrorListener XSLTErrorListener=null;

    private String ConvXMLuseXSLTerr = "";

    //public void setExternalConnectionManager( ThreadSafeClientConnManager externalConnectionManager ) {this.ExternalConnectionManager = externalConnectionManager;}
    //public void setConvXMLuseXSLTerr( String p_ConvXMLuseXSLTerr) { this.ConvXMLuseXSLTerr = p_ConvXMLuseXSLTerr; }

    public  long performMessage(MessageDetails Message, MessageQueueVO messageQueueVO, TheadDataAccess theadDataAccess, Logger MessageSend_Log) {
        // 1. Получаем шаблон обработки для MessageQueueVO
        String SubSys_Cod = messageQueueVO.getSubSys_Cod();
        int MsgDirection_Id = messageQueueVO.getMsgDirection_Id();
        int Operation_Id = messageQueueVO.getOperation_Id();
        Long Queue_Id = messageQueueVO.getQueue_Id();
        String Queue_Direction = messageQueueVO.getQueue_Direction();
        String AnswXSLTQueue_Direction=Queue_Direction;

        String URL_SOAP_Send = "";
        int Function_Result = 0;

        //XSLTErrorListener = new xlstErrorListener();
        //XSLTErrorListener.setXlstError_Log( MessageSend_Log );

        //MonitoringQueueVO monitoringQueueVO = new MonitoringQueueVO();

        MessageSend_Log.info("{} [{}] ищем Шаблон под оперрацию ({}), с учетом системы приёмника MsgDirection_Id={}, SubSys_Cod ={}", Queue_Direction, Queue_Id, Operation_Id, MsgDirection_Id, SubSys_Cod);

        // ищем Шаблон под оперрацию, с учетом системы приёмника MessageRepositoryHelper.look4MessageTemplateVO_2_Perform
        int Template_Id = MessageRepositoryHelper.look4MessageTemplateVO_2_Perform(Operation_Id, MsgDirection_Id, SubSys_Cod, MessageSend_Log);
        MessageSend_Log.info("{} [{}]  Шаблон под оперрацию ={}", Queue_Direction, Queue_Id, Template_Id);

        if ( Template_Id < 0 ) {
            theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO , "Не нашли шаблон обработки сообщения для комбинации: Идентификатор системы[" + MsgDirection_Id + "] Код подсистемы[" + SubSys_Cod + "]", MessageSend_Log);
            // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()), monitoringQueueVO, MessageSend_Log);
            return -11L;
        }

        int messageTypeVO_Key = MessageRepositoryHelper.look4MessageTypeVO_2_Perform( Operation_Id, MessageSend_Log );
        if ( messageTypeVO_Key < 0  ) {
            MessageSend_Log.error( "[{}] MessageRepositoryHelper.look4MessageTypeVO_2_Perform: Не нашли тип сообщения для Operation_Id=[{}]", Queue_Id,  Operation_Id );
            theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO, "Не нашли тип сообщения для Operation_Id=[" + Operation_Id + "]", MessageSend_Log);
            // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessageSend_Log);
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

        int MsgDirectionVO_Key = MessageRepositoryHelper.look4MessageDirectionsVO_2_Perform(MsgDirection_Id, SubSys_Cod, MessageSend_Log);

        if ( MsgDirectionVO_Key >= 0 )
            MessageSend_Log.info("[{}] MsgDirectionVO  getDb_pswd={}{}", Queue_Id, MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getDb_pswd(), MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).LogMessageDirections());
        else {
            MessageSend_Log.error("{} [{}] Не нашли систему-приёмник для пары[{}][{}]", Queue_Direction, Queue_Id, MsgDirection_Id, SubSys_Cod);
            if ( theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO, "Не нашли систему-приёмник для пары[" + MsgDirection_Id + "][" + SubSys_Cod + "]", MessageSend_Log) < 0 )
            {   // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessageSend_Log);
                return -11L;
            }
            // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessageSend_Log);
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
                MessageSend_Log
        );
        MessageSend_Log.info("[{}] MessageTemplate4Perform[{}", Queue_Id, Message.MessageTemplate4Perform.printMessageTemplate4Perform());

        switch (Queue_Direction){
            case XMLchars.DirectOUT:
                // читаем их БД тело XML
                MessageSend_Log.info("[{}] {} : зачитывем из БД тело XML, IsDebugged={}", Queue_Id, Queue_Direction, Message.MessageTemplate4Perform.getIsDebugged());
                MessageUtils.ReadMessage( theadDataAccess, Queue_Id, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log);
                if ( Message.MessageTemplate4Perform.getMessageXSD() != null )
                { boolean is_Message_OUT_Valid;
                    is_Message_OUT_Valid = TestXMLByXSD( Message.XML_MsgOUT.toString(), Message.MessageTemplate4Perform.getMessageXSD(), Message.MsgReason, MessageSend_Log );
                    if ( ! is_Message_OUT_Valid ) {
                        MessageSend_Log.error(" [{}] validateXMLSchema: message `{}` is not valid for XSD`{}`", Queue_Id, Message.XML_MsgOUT.toString(), Message.MessageTemplate4Perform.getMessageXSD());
                        MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess,
                                "validateXMLSchema: message {" + Message.XML_MsgOUT.toString() + "} is not valid for XSD {" + Message.MessageTemplate4Perform.getMessageXSD() + "}" ,
                                null ,  MessageSend_Log);
                        return -1L;
                    }
                }
                // преобразовываем тело
                String MessageXSLT_4_OUT_2_SEND = Message.MessageTemplate4Perform.getMessageXSLT();
                if (( MessageXSLT_4_OUT_2_SEND != null ) && (!MessageXSLT_4_OUT_2_SEND.isEmpty()) )  {
                    String srcXML_4_XSLT_String;
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                        MessageSend_Log.info("[{}] {}  XSLT-преобразователь тела:{{}}", Queue_Id, Queue_Direction, MessageXSLT_4_OUT_2_SEND);
                        // если в ConfigExecute SearchString и Replacement заданы, то заменяем!
                    if (( Message.MessageTemplate4Perform.getPropSearchString() != null ) && ( Message.MessageTemplate4Perform.getPropReplacement() != null ))
                    {
                        if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessageSend_Log.info(Queue_Direction + "[{}] SearchString:`{}`, Replacement:`{}`", Queue_Id, Message.MessageTemplate4Perform.getPropSearchString() , Message.MessageTemplate4Perform.getPropReplacement());
                        srcXML_4_XSLT_String = StringUtils.replace( Message.XML_MsgOUT.toString(),
                                Message.MessageTemplate4Perform.getPropSearchString(),
                                Message.MessageTemplate4Perform.getPropReplacement(),
                                -1);
                    }
                    else srcXML_4_XSLT_String = Message.XML_MsgOUT.toString();
                    try {
                        // Чисто для проверки конструкторов byte[] bb = new Message.XML_MsgOUT;Message.XML_MsgOUT.toString()
                        //StringBuilder xmlStringBuilder = new StringBuilder();
                        //ByteArrayInputStream xmlByteArrayInputStream  = new ByteArrayInputStream( xmlStringBuilder.toString().getBytes("UTF-8") );

                        Message.XML_MsgSEND = ConvXMLuseXSLT30(Queue_Id, srcXML_4_XSLT_String,
                                Message.MessageTemplate4Perform.getMessageXSLT_processor(),
                                Message.MessageTemplate4Perform.getMessageXSLT_xsltCompiler(),
                                Message.MessageTemplate4Perform.getMessageXSLT_xslt30Transformer(),
                                MessageXSLT_4_OUT_2_SEND,
                                Message.MsgReason,
                                MessageSend_Log, Message.MessageTemplate4Perform.getIsDebugged()
                        ) //.substring(XMLchars.xml_xml.length()) // НЕ берем после <?xml version="1.0" encoding="UTF-8"?> , Property.OMIT_XML_DECLARATION = "yes"
                        ;
                        if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessageSend_Log.info(Queue_Direction + " [" + Queue_Id + "] после XSLT=:{" + Message.XML_MsgSEND + "}");
                    } catch ( SaxonApiException exception ) // TransformerException ==> SaxonApiException
                    {
                        MessageSend_Log.error("{} [{}] ConvXMLuseXSLT fault: {}", Queue_Direction, Queue_Id, exception.getMessage());
                        MessageSend_Log.error("{} [{}] XSLT-преобразователь тела:`{}}`", Queue_Direction, Queue_Id, MessageXSLT_4_OUT_2_SEND);
                        MessageSend_Log.error("{} [{}] после XSLT=:{{}}", Queue_Direction, Queue_Id, Message.XML_MsgSEND);
                        MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess,
                                "XSLT fault: message=`" + ConvXMLuseXSLTerr + "` XSLT=`" + srcXML_4_XSLT_String+ "` on " + MessageXSLT_4_OUT_2_SEND ,
                                null ,  MessageSend_Log);
                        // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, srcXML_4_XSLT_String,  Message.XML_MsgOUT.toString(), monitoringQueueVO, MessageSend_Log);
                        return -2L;
                    }
                    if ( Message.XML_MsgSEND.equals(XMLchars.nanXSLT_Result) ) {
                        MessageSend_Log.error("{} [{}] XSLT-преобразователь тела:{{}}", Queue_Direction, Queue_Id, MessageXSLT_4_OUT_2_SEND);
                        MessageSend_Log.error("{} [{}] после XSLT=:`{}`", Queue_Direction, Queue_Id, Message.XML_MsgSEND);
                        MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess,
                                "XSLT fault message: " + ConvXMLuseXSLTerr + srcXML_4_XSLT_String + " on " + MessageXSLT_4_OUT_2_SEND ,
                                null ,  MessageSend_Log);
                        return -201L;
                    }

                     // сохраняем результат XSLT-преобразования( body ) распарсенный по-строчно <Tag><VALUE>
                    if ( MessageUtils.ReplaceMessage4SEND( theadDataAccess, Queue_Id, Message, messageQueueVO, MessageSend_Log)  < 0 )
                    { // Результат преобразования не получилось записать в БД
                        //HE-5864 Спец.символ UTF-16 или любой другой invalid XML character . Ошибка при отправке - удаляет и не записывант сообщение по
                       // Внутри ReplaceMessage4SEND вызов MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO,   Message,  theadDataAccess, "ReplaceMessage4SEND fault" + Message.XML_MsgSEND  ,ex,  MessageSend_Log);
                        return -202L;
                    }
                }       //   MessageXSLT_4_OUT_2_SEND != null
                else
                {  // что на входе, то и отправляем, если нет MessageXSLT для преобразования
                    // ! но если в ConfigExecute SearchString и Replacement заданы, то заменяем!
                    if (( Message.MessageTemplate4Perform.getPropSearchString() != null ) && ( Message.MessageTemplate4Perform.getPropReplacement() != null )) {
                        if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessageSend_Log.info("{} [{}] SearchString:{{}}, Replacement:{{}}", Queue_Direction, Queue_Id, Message.MessageTemplate4Perform.getPropSearchString(), Message.MessageTemplate4Perform.getPropReplacement());

                        Message.XML_MsgSEND = StringUtils.replace( Message.XML_MsgOUT.toString(),
                                Message.MessageTemplate4Perform.getPropSearchString(),
                                Message.MessageTemplate4Perform.getPropReplacement(),
                                -1);
                    }
                    else
                    { Message.XML_MsgSEND = Message.XML_MsgOUT.toString();}
                }
                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessageSend_Log.info("{} [{}] 209 Message.MessageRowNum:{{}}, Message.size:{{}}", Queue_Direction, Queue_Id, Message.MessageRowNum, Message.Message.size());

                // устанавливаем признак "SEND" & COMMIT
                if ( theadDataAccess.doUPDATE_MessageQueue_Out2Send( messageQueueVO, "XSLT (OUT) -> (SEND) ok",  MessageSend_Log) < 0 )
                {
                    return -203L;
                }


            case XMLchars.DirectSEND:
                if ( !Queue_Direction.equals("OUT") ) {
                    // надо читать из БД
                    MessageSend_Log.info("{}-> SEND [{}] читаем SEND БД тело XML", Queue_Direction, Queue_Id);
                    MessageUtils.ReadMessage( theadDataAccess, Queue_Id, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log);
                    if ( Message.MessageRowNum <= 0 ) {
                        MessageSend_Log.error("{}-> SEND [{}] тело XML для SEND в БД пустое !", Queue_Direction, Queue_Id);
                        MessageUtils.ProcessingOutError(  messageQueueVO,   Message,  theadDataAccess,
                                Queue_Direction +"-> SEND ["+ Queue_Id +"] тело XML для SEND в БД пустое !" ,
                                null ,  MessageSend_Log);
                        return -3L;
                    }

                    // Если дата создания до секунд совпала с датой 1-го SEND, значит её ещё не установливали
                    if ( messageQueueVO.getQueue_Create_Date().equals( messageQueueVO.getQueue_Date() ) ) {
                        if ( theadDataAccess.doUPDATE_MessageQueue_Queue_Date4Send(messageQueueVO, MessageSend_Log) < 0 ) {
                            MessageSend_Log.error("{}-> SEND [{}] дата создания до секунд совпала с датой 1-го SEND, значит её ещё не устанавливали!", Queue_Direction, Queue_Id);
                            return -4L;
                        }
                    }

                    Message.XML_MsgSEND = Message.XML_MsgOUT.toString();
                    // Queue_Direction = "SEND";
                }
                Queue_Direction = XMLchars.DirectSEND;
                messageQueueVO.setQueue_Direction(XMLchars.DirectSEND);
                // if ( Queue_Direction.equalsIgnoreCase( "SEND") ) break; // ИЩЕМ Утечку потоков !!!

                // вызов внешней системы
                // провевяем, вдруг это REST
                MessageSend_Log.info("doSEND [{}] getPropWebMetod={} EndPointUrl={} PropTimeout_Conn={} PropTimeout_Read={} Type_Connection={} ShortRetryCount={} LongRetryCount={}",
                        Queue_Id, Message.MessageTemplate4Perform.getPropWebMetod(), Message.MessageTemplate4Perform.getEndPointUrl(), Message.MessageTemplate4Perform.getPropTimeout_Conn(), Message.MessageTemplate4Perform.getPropTimeout_Read(), Message.MessageTemplate4Perform.getType_Connection(), Message.MessageTemplate4Perform.getShortRetryCount(), Message.MessageTemplate4Perform.getLongRetryCount());

                if ((Message.MessageTemplate4Perform.getPropExeMetodExecute() != null) &&
                    (Message.MessageTemplate4Perform.getPropExeMetodExecute().equals(Message.MessageTemplate4Perform.JavaClassExeMetod)) )
                {
                  // 2.1) Это JDBC-обработчик. Используется для организации SQL запроса к "чужой" БД через дополнительный пулл
                  //----------------------------------------------------------------------------
                    if ( ( Message.MessageTemplate4Perform.getEnvelopeXSLTExt() != null ) &&
                         (!Message.MessageTemplate4Perform.getEnvelopeXSLTExt().isEmpty())
                       )
                    { // 2) EnvelopeXSLTExt !! => JDBC-обработчик, причём обращение всегда к ВНЕШНЕЙ БД!

                        if (Message.MessageTemplate4Perform.getIsDebugged()) {
                            MessageSend_Log.info("[{}] Шаблон для SQL-XSLTExt-обработки({})", Queue_Id, Message.MessageTemplate4Perform.getEnvelopeXSLTExt());
                            if (Message.MessageTemplate4Perform.getIsExtSystemAccess()) {
                                MessageSend_Log.info("[{}}] Шаблон для SQL-XSLTExt-обработки использует пулл коннектов для внешней системы({}})", Queue_Id, Message.MessageTemplate4Perform.getEnvelopeXSLTExt());
                            }
                        }
                        String Passed_Envelope4XSLTExt = null;
                        try {
                            Passed_Envelope4XSLTExt = ConvXMLuseXSLT30(Queue_Id,
                                    MessageUtils.PrepareEnvelope4XSLTExt(messageQueueVO, Message.XML_MsgSEND, MessageSend_Log), // Искуственный Envelope/Head/Body + XML_MsgSEND
                                    Message.MessageTemplate4Perform.getEnvelopeXSLTExt_processor(), Message.MessageTemplate4Perform.getEnvelopeXSLTExt_xsltCompiler(),
                                    Message.MessageTemplate4Perform.getEnvelopeXSLTExt_xslt30Transformer(),
                                    Message.MessageTemplate4Perform.getEnvelopeXSLTExt(),  // для диагностики EnvelopeXSLTExt
                                    Message.MsgReason, // результат помещаем сюда
                                    MessageSend_Log, Message.MessageTemplate4Perform.getIsDebugged());
                        } catch (SaxonApiException exception) {
                            MessageSend_Log.error("{} [{}] XSLTExt-преобразователь запроса:{{}}", Queue_Direction, Queue_Id, Message.MessageTemplate4Perform.getEnvelopeXSLTExt());
                            theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT( messageQueueVO, "Ошибка преобразования XSLT для XSLTExt-обработки " + ConvXMLuseXSLTerr.toString() + " :" + Message.MessageTemplate4Perform.getEnvelopeXSLTExt(), 3229,
                                    messageQueueVO.getRetry_Count(), MessageSend_Log);
                            return -31L;
                        }
                        if (Passed_Envelope4XSLTExt.equals(XMLchars.EmptyXSLT_Result)) {
                            MessageSend_Log.error("[{}] Ошибка преобразования XSLT для XSLTExt-обработки {}", Queue_Id, Message.MsgReason.toString());
                            MessageSend_Log.error("[{}] Шаблон для XSLTExt-обработки({})", Queue_Id, Message.MessageTemplate4Perform.getEnvelopeXSLTExt());
                            MessageSend_Log.error("[{}] Envelope4XSLTExt:{}", Queue_Id, ConvXMLuseXSLTerr.toString());
                            theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO, "Ошибка преобразования XSLT для XSLTExt-обработки " + ConvXMLuseXSLTerr.toString() + " :" + Message.MsgReason.toString(), 3231,
                                    messageQueueVO.getRetry_Count(), MessageSend_Log);
                            return -32L;

                        }
                          // специального класса нет -  используем
                         // ExternalXmlSQLStatement.Call4ExternDbSQLincludedXML -- для внещней
                        // или  XmlSQLStatement.ExecuteSQLincludedXML -- для своей БД
                        if (Message.MessageTemplate4Perform.getIsExtSystemAccess()) { // для внещней ExternalXmlSQLStatement.Call4ExternDbSQLincludedXM
                            MessageSend_Log.info("[{}] Шаблон для SQL-XSLTExt-обработки использует пулл коннектов для внешней системы({})", Queue_Id, Message.MessageTemplate4Perform.getEnvelopeXSLTExt());


                            if (Message.MessageTemplate4Perform.getIsDebugged())
                                MessageSend_Log.info("[{}] try ExecuteSQLincludedXML 4 external Db ({})", Queue_Id, Passed_Envelope4XSLTExt);

                            ExtSystemDataConnection extSystemDataConnection = new ExtSystemDataConnection(Queue_Id, MessageSend_Log);
                            if (extSystemDataConnection.ExtSystem_Connection == null) {
                                Message.MsgReason.append("Ошибка на обработке сообщения - ExtSystemDataConnection return: NULL!");
                                return -33L;
                            }
                            Function_Result = ExternalXmlSQLStatement.Call4ExternDbSQLincludedXML(theadDataAccess, extSystemDataConnection.ExtSystem_Connection,
                                    Passed_Envelope4XSLTExt, messageQueueVO, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log);
                            //(theadDataAccess, true, extSystemDataConnection.ExtSystem_Connection ,
                            //        Passed_Envelope4XSLTExt, messageQueueVO, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log);
                            try {
                                extSystemDataConnection.ExtSystem_Connection.close();
                            } catch (SQLException e) {
                                MessageSend_Log.error("[{}] ExtSystem_Connection.close() fault:{}", Queue_Id, e.getMessage());
                            }
                            if (Function_Result != 0) {
                                MessageSend_Log.error("[{}] Envelope4XSLTExt:{}", Queue_Id, ConvXMLuseXSLTerr);
                                MessageSend_Log.error("[{}] Ошибка Call4ExternDbSQLincludedXML:{}", Queue_Id, Message.MsgReason.toString());
                                theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO, "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), 3231,
                                        messageQueueVO.getRetry_Count(), MessageSend_Log);
                                return -34L;
                            } else {
                                if (Message.MessageTemplate4Perform.getIsDebugged())
                                    MessageSend_Log.info("[{}] Исполнение Call4ExternDbSQLincludedXML:{}", Queue_Id, Message.MsgReason.toString());
                            }
                        }
                        else { //XmlSQLStatement.ExecuteSQLincludedXML -- для своей БД
                            if (Message.MessageTemplate4Perform.getIsDebugged())
                                MessageSend_Log.info("[{}] try ExecuteSQLincludedXML 4 internal Db ({})", Queue_Id, Passed_Envelope4XSLTExt);
                            if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                MessageSend_Log.info("{} [{}] 330 Message.MessageRowNum:{{}}, Message.size:{{}}", Queue_Direction, Queue_Id, Message.MessageRowNum, Message.Message.size());
                            Function_Result = XmlSQLStatement.ExecuteSQLincludedXML( theadDataAccess,  Passed_Envelope4XSLTExt, messageQueueVO, Message,
                                                                                     Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log
                                                                                    );
                            if (Function_Result != 0) {
                                MessageSend_Log.error("[{}] Envelope4XSLTExt:{}", Queue_Id , ConvXMLuseXSLTerr);
                                MessageSend_Log.error("[{}] Ошибка ExecuteSQLincludedXML:{}", Queue_Id, Message.MsgReason.toString());
                                theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO, "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(),
                                                                           3231, messageQueueVO.getRetry_Count(), MessageSend_Log);
                                return -34L;
                            } else {
                                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                    MessageSend_Log.info("{} [{}] 340 Message.MessageRowNum:{{}}, Message.size:{{}}", Queue_Direction, Queue_Id, Message.MessageRowNum, Message.Message.size());

                                if (Message.MessageTemplate4Perform.getIsDebugged())
                                    MessageSend_Log.info("[{}] Выполнено ExecuteSQLincludedXML:{}", Queue_Id, Message.MsgReason.toString());
                            }
                            // читаем в XML_ClearBodyResponse , как будто после очистки от внешнего запроса!
                            int ConfirmationRowNum = MessageUtils.ReadConfirmation(theadDataAccess, Queue_Id, Message, MessageSend_Log);
                            if (ConfirmationRowNum < 1) {
                                // Ругаемся, что обработчик не сформировал Confirmation
                                Message.MsgReason.append("[").append(Queue_Id).append("] обработчик не сформировал Confirmation, нарушено соглашение о взаимодействии с Шиной");
                                MessageSend_Log.error("[{}] {}", Queue_Id, Message.MsgReason);
                                theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO, "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(),
                                        3231, messageQueueVO.getRetry_Count(), MessageSend_Log);
                                return -38L;
                            }
                            if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                MessageSend_Log.info("{} [{}] 356 Message.MessageRowNum:{{}}, Message.size:{{}}", Queue_Direction, Queue_Id, Message.MessageRowNum, Message.Message.size());
                        }

                    }
                    else
                    {    // Нет Envelope4XSLTExt - надо орать!
                        MessageSend_Log.error("[{}] В шаблоне для XSLTExt-обработки {} нет Envelope4XSLTExt", Queue_Id, Message.MessageTemplate4Perform.getTemplate_name());
                        theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT( messageQueueVO,
                                "В шаблоне для XSLTExt-обработки " + Message.MessageTemplate4Perform.getTemplate_name() + " нет Envelope4XSLTExt", 3233,
                                messageQueueVO.getRetry_Count(), MessageSend_Log);
                        Message.MsgReason.append( "В шаблоне для XSLTExt-обработки " ).append( Message.MessageTemplate4Perform.getTemplate_name() ).append( " нет Envelope4XSLTExt");
                        return -35L;
                    }                    
                  //----------------------------------------------------------------------------  
                    
                }
                else { // Это может быть ShellScript а также SOAP или Rest

                    if (Message.MessageTemplate4Perform.getPropShellScriptExeFullPathName() != null) {
                        // если указан ShellScript="bash /home/oracle/HE-3997_Hermes_APD_Integration/runDocument2DWH.sh"
                        // TODO
                        Function_Result = ShellScripExecutor.execShell(messageQueueVO, Message, theadDataAccess,
                                MessageSend_Log);
                    } else
                    { // http : SOAP или Rest ( post| get)  если getPropWebMethod() != null
                        // готовим НАБОР заголовков HTTP на основе данных до XSLT преобразования OUT->SEND
                        Message.Soap_HeaderRequest.setLength(0);
                        if (Message.MessageTemplate4Perform.getHeaderXSLT() != null &&
                            Message.MessageTemplate4Perform.getHeaderXSLT().length() > 10) // Есть чем преобразовывать HeaderXSLT
                        {
                            if (Message.MessageTemplate4Perform.getIsDebugged()) {
                                MessageSend_Log.info("{} [{}] XSLT-преобразователь заголовка (чем):`{}`", Queue_Direction, Queue_Id, Message.MessageTemplate4Perform.getHeaderXSLT());
                                MessageSend_Log.info("{} [{}] XSLT-преобразователь заголовка (что):`{}`", Queue_Direction, Queue_Id, Message.XML_MsgOUT);
                            }
                            try {
                                Message.Soap_HeaderRequest.append(
                                        ConvXMLuseXSLT30(messageQueueVO.getQueue_Id(), Message.XML_MsgOUT.toString(), // содержание того, что отправляем
                                                Message.MessageTemplate4Perform.getHeaderXSLT_processor(),
                                                Message.MessageTemplate4Perform.getHeaderXSLT_xsltCompiler(),
                                                Message.MessageTemplate4Perform.getHeaderXSLT_xslt30Transformer(),
                                                Message.MessageTemplate4Perform.getHeaderXSLT(),  // для проверки-диагностики HeaderXSLT
                                                Message.MsgReason, MessageSend_Log,
                                                Message.MessageTemplate4Perform.getIsDebugged()
                                        )
                                        //.substring(XMLchars.xml_xml.length()) // НЕ берем после <?xml version="1.0" encoding="UTF-8"?> , Property.OMIT_XML_DECLARATION = "yes"
                                );
                            } catch (SaxonApiException exception) {
                                MessageSend_Log.error("{} [{}] XSLT-преобразователь заголовка:{{}}", Queue_Direction, Queue_Id, Message.MessageTemplate4Perform.getHeaderXSLT());

                                theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                                        "Header XSLT fault: " + ConvXMLuseXSLTerr + " for " + Message.MessageTemplate4Perform.getHeaderXSLT(), 1244,
                                        messageQueueVO.getRetry_Count(), MessageSend_Log);

                                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, MessageUtils.MakeEntryOutHeader(messageQueueVO, MsgDirectionVO_Key),
                                //        "Header XSLT fault: " + ConvXMLuseXSLTerr  + " for " + Message.MessageTemplate4Perform.getHeaderXSLT(),  monitoringQueueVO, MessageSend_Log);
                                //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, null, null, monitoringQueueVO, MessageSend_Log);
                                return -5L;
                            }
                        }

                        if (Message.MessageTemplate4Perform.getPropWebMetod() != null) {
                            if (Message.MessageTemplate4Perform.getPropWebMetod().equalsIgnoreCase("GET")) {
                                Function_Result = MessageHttpSend.HttpGetMessage(messageQueueVO, Message, theadDataAccess, MessageSend_Log);
                            }
                            if (Message.MessageTemplate4Perform.getPropWebMetod().equalsIgnoreCase("DELETE")) {
                                Function_Result = MessageHttpSend.HttpDeleteMessage(messageQueueVO, Message, theadDataAccess, MessageSend_Log);
                            }
                            //
                            if ( (Message.MessageTemplate4Perform.getPropWebMetod().equalsIgnoreCase("POST")) ||
                                 (Message.MessageTemplate4Perform.getPropWebMetod().equalsIgnoreCase("WEB-FORM"))
                               )
                            {
                                String AckXSLT_4_make_JSON = Message.MessageTemplate4Perform.getAckXSLT() ; // получили XSLT-для
                                String saved_XML_MsgSEND = Message.XML_MsgSEND; // сохранили XML после MessageXSLT но перед  getAckXSLT
                                if ( AckXSLT_4_make_JSON != null ) {
                                    if (Message.MessageTemplate4Perform.getIsDebugged())
                                        MessageSend_Log.info("[{}] PropWebMetod is `{}`, AckXSLT_4_make_JSON ({})", Queue_Id, Message.MessageTemplate4Perform.getPropWebMetod(), AckXSLT_4_make_JSON);
                                    try {
                                        Message.XML_MsgSEND = // make_JSON -> сохраняем для отправки результат преобразования
                                                ConvXMLuseXSLT30(messageQueueVO.getQueue_Id(),
                                                        Message.XML_MsgSEND, // то, что подготовлено для передачи во внешнюю систему в формате XML
                                                        Message.MessageTemplate4Perform.getAckXSLT_processor(),
                                                        Message.MessageTemplate4Perform.getAckXSLT_xsltCompiler(),
                                                        Message.MessageTemplate4Perform.getAckXSLT_xslt30Transformer(),
                                                        AckXSLT_4_make_JSON,  // через HeaderXSLT
                                                        Message.MsgReason, MessageSend_Log,
                                                        Message.MessageTemplate4Perform.getIsDebugged()
                                                );

                                        if (Message.MessageTemplate4Perform.getIsDebugged())
                                            MessageSend_Log.info("[{}] PropWebMetod is `{}`as JSON ({})", Queue_Id, Message.MessageTemplate4Perform.getPropWebMetod(), Message.XML_MsgResponse);

                                    } catch (SaxonApiException exception) {
                                        MessageSend_Log.error("[{}] SEND XSLT-преобразователь для JSON :{{}}", messageQueueVO.getQueue_Id(), AckXSLT_4_make_JSON);

                                        theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                                                "XSLT-преобразователь для JSON fault: " + ConvXMLuseXSLTerr + " for " + AckXSLT_4_make_JSON, 1244,
                                                messageQueueVO.getRetry_Count(), MessageSend_Log);

                                        System.err.println("[" + messageQueueVO.getQueue_Id() + "] AckXSLT: SaxonApi TransformerException ");
                                        exception.printStackTrace();
                                        MessageSend_Log.error("[{}] AckXSLT: from XML `{}` XSLT: `{}` 2 JSON fault:{}", messageQueueVO.getQueue_Id(), Message.XML_MsgSEND, AckXSLT_4_make_JSON, exception.getMessage());
                                        Message.MsgReason.append(" XSLT-преобразователь для JSON `").append(AckXSLT_4_make_JSON).append("` fault: ").append(sStackTrace.strInterruptedException(exception));
                                        MessageUtils.ProcessingSendError(messageQueueVO, Message, theadDataAccess,
                                                "XSLT-преобразователь для JSON", true, exception, MessageSend_Log);
                                        return -402L;
                                    }
                                }
                                if  (Message.MessageTemplate4Perform.getPropWebMetod().equalsIgnoreCase("POST"))
                                    Function_Result = MessageHttpSend.sendPostMessage(messageQueueVO, Message, theadDataAccess, MessageSend_Log);

                                if  (Message.MessageTemplate4Perform.getPropWebMetod().equalsIgnoreCase("WEB-FORM")) {
                                    // для передачи формы используем поле messageTypes.Msg_Type_Own
                                    // если нет специальной разметки типа formDataFieldName="cancelActionDto" в шаблоне
                                    if (Message.MessageTemplate4Perform.getIsDebugged())
                                        MessageSend_Log.info("[{}] WEB-FORM is `{}`fo messageTypeVO_Key = ({})", Queue_Id, messageQueueVO.getMsg_Type_own(), messageTypeVO_Key);

                                    Function_Result = MessageHttpSend.sendWebFormMessage(saved_XML_MsgSEND,
                                            Message.MessageTemplate4Perform.getXPathProcessor(),
                                            Message.MessageTemplate4Perform.getXPathSelector(),
                                            messageQueueVO, Message, theadDataAccess, MessageSend_Log);
                                }

                            }
                            if ( (!Message.MessageTemplate4Perform.getPropWebMetod().equalsIgnoreCase("GET")) &&
                                 (!Message.MessageTemplate4Perform.getPropWebMetod().equalsIgnoreCase("POST")) &&
                                 (!Message.MessageTemplate4Perform.getPropWebMetod().equalsIgnoreCase("DELETE")) &&
                                 (!Message.MessageTemplate4Perform.getPropWebMetod().equalsIgnoreCase("WEB-FORM"))
                               ) {
                                MessageUtils.ProcessingSendError(messageQueueVO, Message, theadDataAccess,
                                        "Свойство WebMetod[" + Message.MessageTemplate4Perform.getPropWebMetod() + "], указанное в шаблоне, не 'get', не 'post', не 'delete' и не `web-form`", true,
                                        null, MessageSend_Log);
                                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_MsgSEND, "Свойство WebMetod["+ Message.MessageTemplate4Perform.getPropWebMetod() + "], указаное в шаблоне не 'get' и не 'post'",  monitoringQueueVO, MessageSend_Log);
                                //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, null, null, monitoringQueueVO, MessageSend_Log);
                                return -401L;
                            }
                        } else { // сообщение Будет отпрвлено через SOAP
                            // готовим SOAP-заголовок
                            Message.Soap_HeaderRequest.setLength(0);
                            if (Message.MessageTemplate4Perform.getHeaderXSLT() != null &&
                                Message.MessageTemplate4Perform.getHeaderXSLT().length() > 10) // Есть чем преобразовывать HeaderXSLT
                                try {
                                    Message.Soap_HeaderRequest.append(
                                            ConvXMLuseXSLT30(messageQueueVO.getQueue_Id(), MessageUtils.MakeEntryOutHeader(messageQueueVO, MsgDirectionVO_Key), // стандартный заголовок c учетом системы-получателя
                                                    Message.MessageTemplate4Perform.getHeaderXSLT_processor(),
                                                    Message.MessageTemplate4Perform.getHeaderXSLT_xsltCompiler(),
                                                    Message.MessageTemplate4Perform.getHeaderXSLT_xslt30Transformer(),
                                                    Message.MessageTemplate4Perform.getHeaderXSLT(),  // через HeaderXSLT
                                                    Message.MsgReason, MessageSend_Log,
                                                    Message.MessageTemplate4Perform.getIsDebugged()
                                            )
                                            //.substring(XMLchars.xml_xml.length()) // НЕ берем после <?xml version="1.0" encoding="UTF-8"?> , Property.OMIT_XML_DECLARATION = "yes"
                                    );
                                } catch (SaxonApiException exception) {
                                    MessageSend_Log.error( "{} [{}}] XSLT-преобразователь заголовка:{ {} } : {}", Queue_Direction, Queue_Id, Message.MessageTemplate4Perform.getHeaderXSLT(), exception.getMessage());

                                    theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                                            "Header XSLT fault: " + ConvXMLuseXSLTerr + " for " + Message.MessageTemplate4Perform.getHeaderXSLT(), 1244,
                                                        messageQueueVO.getRetry_Count(), MessageSend_Log);
                                    return -5L;
                                }
                            else // Make Entry Out Header
                                Message.Soap_HeaderRequest.append(MessageUtils.MakeEntryOutHeader(messageQueueVO, MsgDirectionVO_Key));

                            String AckXSLT_4_make_FinalXML = Message.MessageTemplate4Perform.getAckXSLT() ; // получили XSLT-для
                            if ( AckXSLT_4_make_FinalXML != null ) {
                                if (Message.MessageTemplate4Perform.getIsDebugged()) {
                                    MessageSend_Log.info("[{}] Using SOAP {} XSLT-преобразователь AckXSLT_4_make_FinalXML (чем):`{}`", Queue_Id, Queue_Direction, AckXSLT_4_make_FinalXML);
                                    MessageSend_Log.info("[{}] Using SOAP {} XSLT-преобразователь AckXSLT_4_make_FinalXML (что):`{}`", Queue_Id, Queue_Direction, Message.XML_MsgSEND);
                                }
                                if (Message.MessageTemplate4Perform.getIsDebugged())
                                    MessageSend_Log.info("[{}] Using SOAP and use AckXSLT_4_make_FinalXML ({})", Queue_Id, AckXSLT_4_make_FinalXML);
                                try {
                                    Message.XML_MsgSEND = // make_FinalXML body -> сохраняем для отправки результат преобразования
                                            XML.unescape(
                                            ConvXMLuseXSLT30(messageQueueVO.getQueue_Id(),
                                                    Message.XML_MsgSEND, // то, что подготовлено для передачи во внешнюю систему в формате XML
                                                    Message.MessageTemplate4Perform.getAckXSLT_processor(),
                                                    Message.MessageTemplate4Perform.getAckXSLT_xsltCompiler(),
                                                    Message.MessageTemplate4Perform.getAckXSLT_xslt30Transformer(),
                                                    AckXSLT_4_make_FinalXML,  // через AckXSLT
                                                    Message.MsgReason, MessageSend_Log,
                                                    Message.MessageTemplate4Perform.getIsDebugged()
                                                )
                                            );

                                    if (Message.MessageTemplate4Perform.getIsDebugged())
                                        MessageSend_Log.info("[{}] Using SOAP and use AckXSLT_4_make_FinalXML, (получили):`{}`", Queue_Id, Message.XML_MsgSEND);

                                } catch (SaxonApiException exception) {
                                    MessageSend_Log.error("[{}] SEND  XSLT-преобразователь для FinalXML :{{}}", messageQueueVO.getQueue_Id(), AckXSLT_4_make_FinalXML);

                                    theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                                            "Header XSLT fault: " + ConvXMLuseXSLTerr + " for " + AckXSLT_4_make_FinalXML, 1244,
                                            messageQueueVO.getRetry_Count(), MessageSend_Log);

                                    System.err.println("[" + messageQueueVO.getQueue_Id() + "] AckXSLT: SaxonApi TransformerException ");
                                    exception.printStackTrace();
                                    MessageSend_Log.error("[{}] AckXSLT: from XML `{}` XSLT: `{}` 2 FinalXML fault:{}", messageQueueVO.getQueue_Id(), Message.XML_MsgSEND, AckXSLT_4_make_FinalXML, exception.getMessage());
                                    Message.MsgReason.append(" XSLT-преобразователь для FinalXML `").append(AckXSLT_4_make_FinalXML).append("` fault: ").append(sStackTrace.strInterruptedException(exception));
                                    MessageUtils.ProcessingSendError(messageQueueVO, Message, theadDataAccess,
                                            "XSLT-преобразователь для FinalXML", true, exception, MessageSend_Log);
                                    return -402L;
                                }
                            }
                            // Собственно, ВЫЗОВ! XML_MsgSEND as Soap body
                            Function_Result = MessageHttpSend.sendSoapMessage(messageQueueVO, Message, theadDataAccess, MessageSend_Log);
                            // MessageSend_Log.info("sendSOAPMessage:" + Queue_Direction + " [" + Queue_Id + "] для SOAP=:\n" + Message.XML_MsgSEND);
                        }
                    }
                }
                if ( Function_Result <0 ) {
                    // TODO
                    // Надо бы всзести переменную - что c Http всё плохо, но пост-обработчик надо всё же вызвать хоть раз.
                     AnswXSLTQueue_Direction = messageQueueVO.getQueue_Direction();
                    break;
                }
                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessageSend_Log.info("{} [{}] 514 Message.MessageRowNum:{{}}, Message.size:{{}}", Queue_Direction, Queue_Id, Message.MessageRowNum, Message.Message.size());


                // шаблон MsgAnswXSLT заполнен
                if ( Message.MessageTemplate4Perform.getMsgAnswXSLT() != null) {
                    if ( Message.MessageTemplate4Perform.getIsDebugged()  ) {
                        MessageSend_Log.info("{} [{}] MsgAnswXSLT: {}", Queue_Direction, Queue_Id, Message.MessageTemplate4Perform.getMsgAnswXSLT());
                    }
                    try {
                    Message.XML_MsgRESOUT.append(
                            ConvXMLuseXSLT30(
                                    Queue_Id, Message.XML_ClearBodyResponse.toString(), // очищенный от ns: /Envelope/Body
                                    Message.MessageTemplate4Perform.getMsgAnswXSLT_processor(),
                                    Message.MessageTemplate4Perform.getMsgAnswXSLT_xsltCompiler(),
                                    Message.MessageTemplate4Perform.getMsgAnswXSLT_xslt30Transformer(),
                                    Message.MessageTemplate4Perform.getMsgAnswXSLT(),  // через MsgAnswXSLT
                                    Message.MsgReason, MessageSend_Log,
                                    Message.MessageTemplate4Perform.getIsDebugged()
                                    )
                            //.substring(XMLchars.xml_xml.length()) // НЕ берем после <?xml version="1.0" encoding="UTF-8"?> , Property.OMIT_XML_DECLARATION = "yes"
                    );
                    } catch ( Exception exception ) {
                        MessageSend_Log.error("{} [{}] XSLT-преобразователь ответа:{{}}", Queue_Direction, Queue_Id, Message.MessageTemplate4Perform.getMsgAnswXSLT());

                        theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO, //.getQueue_Id(),
                                "Answer XSLT fault: " + ConvXMLuseXSLTerr  + " on " + Message.MessageTemplate4Perform.getMsgAnswXSLT(), 1243,
                                messageQueueVO.getRetry_Count(), MessageSend_Log);

                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO,Message.XML_ClearBodyResponse.toString(),
                        //        "Answer XSLT fault: " + ConvXMLuseXSLTerr  + " on " + Message.MessageTemplate4Perform.getMsgAnswXSLT(),  monitoringQueueVO, MessageSend_Log);
                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                        return -501L;
                    }
                    // MessageSend_Log.info(Queue_Direction +" ["+ Queue_Id +"] Message.MessageTemplate4Perform.getIsDebugged()=" + Message.MessageTemplate4Perform.getIsDebugged() );
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                        MessageSend_Log.info("{} [{}] преобразовали XML-ответ в: {}", Queue_Direction, Queue_Id, Message.XML_MsgRESOUT.toString());
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                        MessageSend_Log.info("{} [{}] 550 Message.MessageRowNum:{{}}, Message.size:{{}}", Queue_Direction, Queue_Id, Message.MessageRowNum, Message.Message.size());
                }
                else // берем как есть без преобразования
                {
                    Message.XML_MsgRESOUT.append(Message.XML_ClearBodyResponse.toString());
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                        MessageSend_Log.info("{} [{}] используем XML-ответ как есть без преобразования:({})", Queue_Direction, Queue_Id, Message.XML_MsgRESOUT.toString());
                    if ( Message.MessageTemplate4Perform.getIsDebugged() )
                        MessageSend_Log.info("{} [{}] 559 Message.MessageRowNum:{{}}, Message.size:{{}}", Queue_Direction, Queue_Id, Message.MessageRowNum, Message.Message.size());

                }
                    // Проверяем наличие TagNext ="Next" в XML_MsgRESOUT
                AnswXSLTQueue_Direction = MessageUtils.PrepareConfirmation(  theadDataAccess,  messageQueueVO,  Message, Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log );
                messageQueueVO.setQueue_Direction(AnswXSLTQueue_Direction);

                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessageSend_Log.info("{} [{}] 557 Message.MessageRowNum:{{}}, Message.size:{{}}", Queue_Direction, Queue_Id, Message.MessageRowNum, Message.Message.size());


                if ( !AnswXSLTQueue_Direction.equals(XMLchars.DirectRESOUT))
                    // TODO Надо бы всзести переменную - что c XSLT всё плохо, но пост-обработчик надо всё же вызвать хоть раз.
                {  // перечитываем состояние заголовка сообщения из БД
                    theadDataAccess.do_SelectMESSAGE_QUEUE(  messageQueueVO, MessageSend_Log );
                    break;
                }

                messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
                messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
                messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());

                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_ClearBodyResponse.toString(), Message.XML_MsgRESOUT.toString(),  monitoringQueueVO, MessageSend_Log);
                //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);

                // получение и преобразование результатов
            case XMLchars.DirectRESOUT : //"RESOUT"
                // проверяем НАЛИЧИЕ пост-обработчика в Шаблоне
                if ( Message.MessageTemplate4Perform.getConfigPostExec() != null ) { // 1) ConfigPostExec
                    if ( !Queue_Direction.equals("SEND") ) {
                        // надо читать из БД
                        MessageSend_Log.error("{}-> DELOUT/ATTOUT/ERROUT [{}] читаем SEND БД тело XML", Queue_Direction, Queue_Id);
                        MessageUtils.ReadMessage( theadDataAccess, Queue_Id, Message, Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log);
                        Message.XML_MsgSEND = Message.XML_MsgOUT.toString();
                        Queue_Direction = XMLchars.DirectPOSTOUT;
                        MessageSend_Log.error("[{}] Этот код для повторнй обработки Ответв на Исходящее событие ещё не написан.  ", Queue_Id);
                        theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                "Этот код для повторнй обработки Ответв на Исходящее событие ещё не написан. Сделано от защиты зацикливания", 1232,
                                messageQueueVO.getRetry_Count(),  MessageSend_Log);
                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.XML_MsgSEND,
                        //        "Этот код для повторнй обработки Ответв на Исходяе событие ещё не написан. Сделано от защиты зацикливания",  monitoringQueueVO, MessageSend_Log);
                        //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                        return -11L;
                    }
                    // TODO Надо бы всзести переменную - что  пост-обработчик  всё же вызвался хоть раз и если ошибка, то больше не надо.
                    messageQueueVO.setQueue_Direction(XMLchars.DirectRESOUT);
                    if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.JavaClassExeMetod) )
                    { // 2.1) Это JDBC-обработчик
                        if ( Message.MessageTemplate4Perform.getEnvelopeXSLTPost() != null ) { // 2) EnvelopeXSLTPost
                            if (!Message.MessageTemplate4Perform.getEnvelopeXSLTPost().isEmpty()) {
                                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                    MessageSend_Log.info("[{}] Шаблон EnvelopeXSLTPost для пост-обработки({})", Queue_Id, Message.MessageTemplate4Perform.getEnvelopeXSLTPost());
                                if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                    MessageSend_Log.info("[{}] Envelope4XSLTPost:{}", Queue_Id, MessageUtils.PrepareEnvelope4XSLTPost(messageQueueVO, Message, MessageSend_Log));

                                String Passed_Envelope4XSLTPost;
                                try {
                                    Passed_Envelope4XSLTPost= ConvXMLuseXSLT30(messageQueueVO.getQueue_Id(),
                                            MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO, Message, MessageSend_Log), // Искуственный Envelope/Head/Body is XML_MsgRESOUT
                                            Message.MessageTemplate4Perform.getEnvelopeXSLTPost_processor(),
                                            Message.MessageTemplate4Perform.getEnvelopeXSLTPost_xsltCompiler(),
                                            Message.MessageTemplate4Perform.getEnvelopeXSLTPost_xslt30Transformer(),
                                            Message.MessageTemplate4Perform.getEnvelopeXSLTPost(),  // через EnvelopeXSLTPost
                                            Message.MsgReason, MessageSend_Log, Message.MessageTemplate4Perform.getIsDebugged());
                                } catch ( SaxonApiException exception ) {
                                    MessageSend_Log.error("{} [{}] XSLT-пост-преобразователь ответа:{{}}", Queue_Direction, Queue_Id, Message.MessageTemplate4Perform.getEnvelopeXSLTPost());
                                    theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                            "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost(), 1235,
                                            messageQueueVO.getRetry_Count(),  MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO,  Message, MessageSend_Log),
                                    //        "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getEnvelopeXSLTPost(),  monitoringQueueVO, MessageSend_Log);
                                    return -101L;
                                }
                                if ( Passed_Envelope4XSLTPost.equals(XMLchars.EmptyXSLT_Result))
                                {
                                    MessageSend_Log.error("[{}] Шаблон для пост-обработки({})", Queue_Id, Message.MessageTemplate4Perform.getEnvelopeXSLTPost());
                                    MessageSend_Log.error("[{}}] Envelope4XSLTPost:{}" , Queue_Id, MessageUtils.PrepareEnvelope4XSLTPost(  messageQueueVO,  Message, MessageSend_Log) );
                                    MessageSend_Log.error("[{}}] Ошибка преобразования XSLT для пост-обработки {}", Queue_Id, Message.MsgReason.toString() );
                                    theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                            "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(), 1232,
                                            messageQueueVO.getRetry_Count(),  MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Passed_Envelope4XSLTPost,
                                    //        "Ошибка преобразования XSLT для пост-обработки " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(),  monitoringQueueVO, MessageSend_Log);
                                    return -12L;

                                }

                                final int resultSQL = XmlSQLStatement.ExecuteSQLincludedXML( theadDataAccess,  Passed_Envelope4XSLTPost, messageQueueVO, Message,
                                                                                             Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log
                                                                                          );
                                if (resultSQL != 0) {
                                    MessageSend_Log.error("[{}] Envelope4XSLTPost:{}",Queue_Id , MessageUtils.PrepareEnvelope4XSLTPost( messageQueueVO,  Message, MessageSend_Log) );
                                    MessageSend_Log.error("[{}] Ошибка ExecuteSQLinXML({}):{}", Queue_Id, resultSQL, Message.MsgReason.toString());
                                    theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                            "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), 1232,
                                            messageQueueVO.getRetry_Count(),  MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Passed_Envelope4XSLTPost,
                                    //        "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(),  monitoringQueueVO, MessageSend_Log);
                                    return -13L;
                                }
                                else
                                {if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                    MessageSend_Log.info("[{}] Исполнение Envelope4XSLTPost ExecuteSQLinXML:{}", Queue_Id, Message.MsgReason.toString() );
                                    // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                                    /*if ( theadDataAccess.do_SelectMESSAGE_QUEUE(  messageQueueVO, MessageSend_Log ) == 0 )
                                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4XSLTPost,
                                                "ExecuteSQLinXML: " + Message.MsgReason.toString(),  monitoringQueueVO, MessageSend_Log);
                                    else
                                        ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4XSLTPost,
                                                "do_SelectMESSAGE_QUEUE fault " ,  monitoringQueueVO, MessageSend_Log);
                                     */
                                }
                            }
                            else
                            {   // Нет EnvelopeXSLTPost - надо орать! прописан Java класс, а EnvelopeXSLTPost нет
                                MessageSend_Log.error("[{}] В для пост-обработки ExeMetod=`{}`, но нет EnvelopeXSLTPost", Queue_Id, Message.MessageTemplate4Perform.getPropExeMetodPostExec());
                                theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                        "В шаблоне для пост-обработки ExeMetod=`" + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + "`, но нет EnvelopeXSLTPost", 1232,
                                        messageQueueVO.getRetry_Count(),  MessageSend_Log);
                                return -14L;
                            }
                        }
                        else
                        {
                            // Нет EnvelopeXSLTPost - надо орать!
                            MessageSend_Log.error("[{}] В Not-Null шаблоне для пост-обработки {} нет EnvelopeXSLTPost", Queue_Id, Message.MessageTemplate4Perform.getPropExeMetodPostExec());
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "В Not-Null шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost", 1232,
                                    messageQueueVO.getRetry_Count(),  MessageSend_Log);
                            // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.MessageTemplate4Perform.getPropExeMetodPostExec(),
                            //        "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет EnvelopeXSLTPost",  monitoringQueueVO, MessageSend_Log);
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
                            MessageSend_Log.error("[{}] В шаблоне для пост-обработки {} нет параметров для Rest-HttpGet включая логин/пароль", Queue_Id, Message.MessageTemplate4Perform.getPropExeMetodPostExec());
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet включая логин/пароль", 1232,
                                    messageQueueVO.getRetry_Count(),  MessageSend_Log);
                            // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.MessageTemplate4Perform.getPropExeMetodPostExec(),
                            //        "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet включая логин/пароль",  monitoringQueueVO, MessageSend_Log);
                            return -16L;
                        }
                       long result_WebRestExePostExec = MessageHttpSend.WebRestExePostExec(messageQueueVO,
                                                                                           Message.MessageTemplate4Perform, // Message.RestHermesAPIHttpClient,
                                                                                           theadDataAccess, Message.ApiRestWaitTime,
                                                                                           MessageSend_Log );
                        if ( result_WebRestExePostExec < 0L )
                            return result_WebRestExePostExec;
                    }

                }
                else
                {if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    MessageSend_Log.info("[{}] ExeMetod для пост-обработки({})", Queue_Id, Message.MessageTemplate4Perform.getPropExeMetodPostExec());
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
            MessageSend_Log.info("[{}] string 759:", Queue_Id);
            MessageSend_Log.info("[{}] AnswXSLTQueue_Direction='{}'", Queue_Id, AnswXSLTQueue_Direction);
            MessageSend_Log.info("[{}] messageQueueVO.getQueue_Direction()='{}'", Queue_Id, messageQueueVO.getQueue_Direction());
        }

        if ( AnswXSLTQueue_Direction.equals(XMLchars.DirectERROUT)
        && !messageQueueVO.getQueue_Direction().equals(XMLchars.DirectRESOUT)) {
            if ( Message.MessageTemplate4Perform.getIsDebugged() )
                MessageSend_Log.info("[{}] ExeMetod для пост-обработки сообщения, получившего ошибку от внешней системы ({})", Queue_Id, Message.MessageTemplate4Perform.getPropExeMetodPostExec());
            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec() != null ) // если  пост-обработчик вообще указан !
            // вызов пост-обработчика ??? - вызов при необходимости, ноавая фича
            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.WebRestExeMetod) )
            { // 2.2) Это Rest-HttpGet-вызов

                if (( Message.MessageTemplate4Perform.getPropHostPostExec() == null ) ||
                        ( Message.MessageTemplate4Perform.getPropUserPostExec() == null ) ||
                        ( Message.MessageTemplate4Perform.getPropPswdPostExec() == null ) ||
                        ( Message.MessageTemplate4Perform.getPropUrlPostExec()  == null ) )
                {
                    // Нет параметров для Rest-HttpGet - надо орать!
                    MessageSend_Log.error("["+ Queue_Id +"] В шаблоне для пост-обработки статуса " + XMLchars.DirectERROUT + " " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet вклюая логин/пароль");
                    theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                            "В шаблоне для пост-обработки статуса " + XMLchars.DirectERROUT + " " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet вклюая логин/пароль", 1232,
                            messageQueueVO.getRetry_Count(),  MessageSend_Log);
                    // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                    //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, Message.MessageTemplate4Perform.getPropExeMetodPostExec(),
                    //        "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет параметров для Rest-HttpGet вклюая логин/пароль",  monitoringQueueVO, MessageSend_Log);
                    return -161L;
                }
                Long WebRestErrOUTPostExecResult= MessageHttpSend.WebRestErrOUTPostExec( messageQueueVO, Message.MessageTemplate4Perform, Message.ApiRestWaitTime,
                                                    theadDataAccess, MessageSend_Log );
                if ( WebRestErrOUTPostExecResult != 0L )
                    return WebRestErrOUTPostExecResult;
            }
            //---------------------
            if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec() != null ) // если  пост-обработчик вообще указан !
                // ExeMetod в ConfigPostExec.prop не обязательно должен быть "java-class", он может быть и  web-rest, убираем это условие
                // if ( Message.MessageTemplate4Perform.getPropExeMetodPostExec().equals(Message.MessageTemplate4Perform.JavaClassExeMetod) )
            {
                if (Message.MessageTemplate4Perform.getErrTransXSLT() != null) { // 2) getErrTransXSLT
                    if (!Message.MessageTemplate4Perform.getErrTransXSLT().isEmpty()) {
                        if (Message.MessageTemplate4Perform.getIsDebugged())
                            MessageSend_Log.info("[{}] Шаблон ErrTransXSLT для пост-обработки({})", Queue_Id, Message.MessageTemplate4Perform.getErrTransXSLT());
                        if (Message.MessageTemplate4Perform.getIsDebugged())
                            MessageSend_Log.info("[{}] ErrTransXSLT:{}", Queue_Id, MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessageSend_Log));

                        String Passed_Envelope4ErrTransXSLT;
                        try {
                            Passed_Envelope4ErrTransXSLT = ConvXMLuseXSLT30(messageQueueVO.getQueue_Id(),
                                    MessageUtils.PrepareEnvelope4ErrTransXSLT( messageQueueVO, Message, MessageSend_Log), // Искуственный Envelope/Head/Body is XML_MsgRESOUT
                                    Message.MessageTemplate4Perform.getErrTransXSLT_processor(),
                                    Message.MessageTemplate4Perform.getErrTransXSLT_xsltCompiler(),
                                    Message.MessageTemplate4Perform.getErrTransXSLT_xslt30Transformer(),
                                    Message.MessageTemplate4Perform.getErrTransXSLT(),  // через getErrTransXSLT
                                    Message.MsgReason, MessageSend_Log, Message.MessageTemplate4Perform.getIsDebugged());
                        } catch (SaxonApiException exception) {
                            MessageSend_Log.error("{} [{}] XSLT для обработки ERROUT ответа:{{}}", Queue_Direction, Queue_Id, Message.MessageTemplate4Perform.getErrTransXSLT());
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "Ошибка преобразования XSLT для обработки ERROUT" + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getErrTransXSLT(), 1295,
                                    messageQueueVO.getRetry_Count(), MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessageSend_Log),
                            //        "Ошибка преобразования XSLT для обработки ERROUT " + ConvXMLuseXSLTerr + " :" + Message.MessageTemplate4Perform.getErrTransXSLT(), monitoringQueueVO, MessageSend_Log);
                            return -18L;
                        }
                        if (Passed_Envelope4ErrTransXSLT.equals(XMLchars.EmptyXSLT_Result)) {
                            MessageSend_Log.error("[{}] Шаблон ErrTransXSLT для обработки ERROUT({})", Queue_Id, Message.MessageTemplate4Perform.getErrTransXSLT());
                            MessageSend_Log.error("[{}] Envelope4ErrTransXSLT:{}", Queue_Id, MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessageSend_Log));
                            MessageSend_Log.error("[{}] Ошибка преобразования XSLT для обработки ERROUT {}", Queue_Id, Message.MsgReason.toString());
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "Ошибка преобразования XSLT для обработки ERROUT " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(), 1292,
                                    messageQueueVO.getRetry_Count(), MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, Passed_Envelope4ErrTransXSLT,
                            //        "Ошибка преобразования XSLT для обработки ERROUT " + ConvXMLuseXSLTerr + " :" + Message.MsgReason.toString(), monitoringQueueVO, MessageSend_Log);
                            return -19L;

                        }

                        final int resultSQL = XmlSQLStatement.ExecuteSQLincludedXML(theadDataAccess, Passed_Envelope4ErrTransXSLT, messageQueueVO, Message,
                                                                                    Message.MessageTemplate4Perform.getIsDebugged(), MessageSend_Log
                                                                                    );
                        if (resultSQL != 0) {
                            MessageSend_Log.error("[{}] ErrTransXSLT Envelope4XSLTPost:{}", Queue_Id, MessageUtils.PrepareEnvelope4ErrTransXSLT(messageQueueVO, Message, MessageSend_Log));
                            MessageSend_Log.error("[{}] Ошибка ExecuteSQLinXML:{}", Queue_Id, Message.MsgReason.toString());
                            theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                    "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), 1292,
                                    messageQueueVO.getRetry_Count(), MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                            //ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, Passed_Envelope4ErrTransXSLT,
                            //        "Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), monitoringQueueVO, MessageSend_Log);
                            return -20L;
                        } else {
                            if (Message.MessageTemplate4Perform.getIsDebugged())
                                MessageSend_Log.info("[" + Queue_Id + "] Исполнение ErrTransXSLT ExecuteSQLinXML:" + Message.MsgReason.toString());
                            //ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, null, null,  monitoringQueueVO, MessageSend_Log);
                            /*if (theadDataAccess.do_SelectMESSAGE_QUEUE(messageQueueVO, MessageSend_Log) == 0)
                                ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4ErrTransXSLT,
                                        "ExecuteSQLinXML: " + Message.MsgReason.toString(), monitoringQueueVO, MessageSend_Log);
                            else
                                ConcurrentQueue.addMessageQueueVO2queue(messageQueueVO, "Исполнение ExecuteSQLinXML:" + Passed_Envelope4ErrTransXSLT,
                                        "do_SelectMESSAGE_QUEUE fault ", monitoringQueueVO, MessageSend_Log);
                            */
                        }
                    } else {   // Нет ErrTransXSLT - надо орать!  EnvelopeXSLTPost пустой
                        MessageSend_Log.error("[{}] В Not-NUll шаблоне для обработки ERROUT {} нет ErrTransXSLT", Queue_Id, Message.MessageTemplate4Perform.getPropExeMetodPostExec());
                        theadDataAccess.doUPDATE_MessageQueue_Send2AttOUT(messageQueueVO,
                                "В Not-NUll шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет ErrTransXSLT", 1292,
                                messageQueueVO.getRetry_Count(), MessageSend_Log);
                        return -21L;
                    }
                }
                else {
                    MessageSend_Log.warn("[{}] для обработки ERROUT {} В шаблоне нет секции ErrTransXSLT", Queue_Id, Message.MessageTemplate4Perform.getPropExeMetodPostExec());
                }
            }
            //-----------------------
        }

/*
        try {  SimpleHttpClient.close(); } catch ( IOException e) {
           MessageSend_Log.error("под конец  ошибка SimpleHttpClient.close(): " + e.getMessage() );
            Message.SimpleHttpClient = null;
            return messageQueueVO.getQueue_Id();      }
*/

        return messageQueueVO.getQueue_Id();
    }

   // private Security endpointProperties;
/*
    private void registerTlsScheme(SchemeLayeredSocketFactory factory, int port) {
        Scheme sch = new Scheme(HTTPS, port, factory);
        client.getConnectionManager().getSchemeRegistry().register(sch);
    }
*/
    private  boolean TestXMLByXSD(@NotNull String xmldata, @NotNull String XSDdata, StringBuilder MsgResult,  Logger MessageSend_Log)// throws Exception
    {
        Validator valid=null;
        StreamSource reqwsdl=null, xsdss = null;
        Schema XSDshema= null;

        try
        { reqwsdl = new StreamSource(new ByteArrayInputStream(xmldata.getBytes()));
            xsdss   = new StreamSource(new ByteArrayInputStream(XSDdata.getBytes()));
            XSDshema = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(xsdss);
            valid =XSDshema.newValidator();
            valid.validate(reqwsdl);

        }
        catch ( Exception exp ) {
            MessageSend_Log.error("Exception: {}", exp.getMessage());
            MsgResult.setLength(0);
            MsgResult.append( "TestXMLByXSD:"); MsgResult.append( sStackTrace.strInterruptedException(exp) );
            return false;}
        MessageSend_Log.info("validateXMLSchema message\n{}\n is VALID for XSD\n{}", xmldata, XSDdata);
        return true;
    }


    private String ConvXMLuseXSLT10(@NotNull Long QueueId, @NotNull String xmldata,
                                    @NotNull String XSLTdata,
                                    StringBuilder MsgResult, Logger MessageSend_Log, boolean IsDebugged )
            throws SaxonApiException // TransformerException
    { StreamSource xmlStreamSource ,srcXSLT;
        //Transformer transformer;
        // StreamResult result;
        ByteArrayInputStream xmlInputStream=null;
        //BufferedInputStream  _xmlInputStream;
        ByteArrayOutputStream outputByteArrayStream =new ByteArrayOutputStream();
        String res=XMLchars.EmptyXSLT_Result;
        ConvXMLuseXSLTerr="";
        try {
            xmlInputStream  = new ByteArrayInputStream( xmldata.getBytes(StandardCharsets.UTF_8) );
        }
        catch ( Exception exp ) {
            ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
            exp.printStackTrace();
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.ByteArrayInputStream Exception" );
            MessageSend_Log.error("["+ QueueId  + "] Exception: {}" , ConvXMLuseXSLTerr );
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT:");  MsgResult.append( ConvXMLuseXSLTerr );
            return XMLchars.EmptyXSLT_Result ;
        }

        if ( (XSLTdata == null) || ( XSLTdata.length() < XMLchars.EmptyXSLT_Result.length() )  ) {
            ConvXMLuseXSLTerr = " length XSLTdata 4 transform is null OR  < " + XMLchars.EmptyXSLT_Result.length();
            if ( IsDebugged )
                MessageSend_Log.info("[{}] length XSLTdata 4 transform is null OR  < {}", QueueId, XMLchars.EmptyXSLT_Result.length());
            return XMLchars.EmptyXSLT_Result ;
        }


        xmlStreamSource = new StreamSource(xmlInputStream);
        try {
            srcXSLT = new StreamSource(new ByteArrayInputStream(XSLTdata.getBytes(StandardCharsets.UTF_8)));
        }
                catch ( Exception exp ) {
                ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);

                System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.ByteArrayInputStream Exception:" );
                    exp.printStackTrace();
                MessageSend_Log.error("[{}] Exception: {}", QueueId , ConvXMLuseXSLTerr );
                MsgResult.setLength(0);
                MsgResult.append( "ConvXMLuseXSLT:");  MsgResult.append( ConvXMLuseXSLTerr );
                return XMLchars.EmptyXSLT_Result ;
            }

        try
        {

            Processor processor = new Processor(false);
            XsltCompiler xsltCompiler = processor.newXsltCompiler();
            XsltExecutable xsltStylesheet = xsltCompiler.compile( srcXSLT );
            Xslt30Transformer xslt30Ttransformer = xsltStylesheet.load30();
            if (IsDebugged)
            MessageSend_Log.warn("["+ QueueId  + "] using XsltLanguageVersion {}",  xsltCompiler.getXsltLanguageVersion() ) ;
            Serializer outSerializer = processor.newSerializer();
            outSerializer.setOutputProperty(Serializer.Property.METHOD, "xml");
            outSerializer.setOutputProperty(Serializer.Property.ENCODING, "utf-8");
            outSerializer.setOutputProperty(Serializer.Property.INDENT, "no");
            outSerializer.setOutputProperty(Serializer.Property.OMIT_XML_DECLARATION, "no");
            outSerializer.setOutputStream(outputByteArrayStream);
            // outputByteArrayStream

            xslt30Ttransformer.transform( xmlStreamSource, outSerializer);


                res = outputByteArrayStream.toString();
                if (!res.isEmpty()) {
                    // System.err.println("result != null, res:" + res );
                    if ((res.charAt(0) == '{') || (res.charAt(0) == '[')) {
                        if (IsDebugged)
                            MessageSend_Log.warn("[{}] json transformer.transform(`{}`)", QueueId, res);
                    } else if (res.length() < XMLchars.EmptyXSLT_Result.length()) {
                        ConvXMLuseXSLTerr = " length Xtransformer.transform(`" + res + "`) < " + XMLchars.EmptyXSLT_Result.length();
                        if (IsDebugged)
                            MessageSend_Log.warn("[{}] length transformer.transform(`{}`) < {}", QueueId, res, XMLchars.EmptyXSLT_Result.length());
                        res = XMLchars.EmptyXSLT_Result;
                    }
                }
                else {
                    ConvXMLuseXSLTerr = " length Xtransformer.transform(`" + res + "`) == 0 ";
                    if (IsDebugged)
                        MessageSend_Log.warn("[{}] length transformer.transform(`{}`) == 0", QueueId, res);
                    res = XMLchars.EmptyXSLT_Result;
                }
            /*
                if ( IsDebugged ) {
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML IN ): " + xmldata);
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML out ): " + res);
            }
            */
        }
        catch ( SaxonApiException exp ) {
            ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.Transformer TransformerException" );
            exp.printStackTrace();
            MessageSend_Log.error("[{}] ConvXMLuseXSLT.Transformer TransformerException: {}", QueueId, ConvXMLuseXSLTerr);
            if (  !IsDebugged ) {
                MessageSend_Log.error("[{}] ConvXMLuseXSLT( XML IN ): {}", QueueId, xmldata);
                MessageSend_Log.error("[{}] ConvXMLuseXSLT( XSLT ): {}", QueueId, XSLTdata);
                MessageSend_Log.error("[{}] ConvXMLuseXSLT( XML out ): {}", QueueId, res);
            }
            MessageSend_Log.error("["+ QueueId  + "] Transformer.Exception: " + ConvXMLuseXSLTerr);
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT.Transformer:");  MsgResult.append( ConvXMLuseXSLTerr );
            throw exp;
            // return XMLchars.EmptyXSLT_Result ;
        }
        return(res);
    }

    private String ConvXMLuseXSLT30(@NotNull Long QueueId, @NotNull String XMLdata_4_Tranform,
                                    @NotNull Processor xslt30Processor, @NotNull XsltCompiler xslt30Compiler,
                                    @NotNull Xslt30Transformer xslt30Transformer,
                                    @NotNull String checkXSLTtext, StringBuilder MsgResult, Logger MessageSend_Log, boolean IsDebugged )
            throws SaxonApiException // TransformerException
    { StreamSource xmlStreamSource; // ,srcXSLT;
        //Transformer transformer;
        // StreamResult result;

        if ( (checkXSLTtext != null) && ( !checkXSLTtext.isEmpty() ) &&
             (xslt30Transformer == null) // проверяем, получилось ли из проверяемого XSLT скомпилировать xslt30Transformer на этапе загрузки
           ) {
            ConvXMLuseXSLTerr = " ConvXMLuseXSLT30:: length XSLTdata 4 transform is NOT NULL (OR < " + XMLchars.EmptyXSLT_Result.length() + "), but xslt30Processor/xslt30Transformer == null";

            MessageSend_Log.error("[{}] ConvXMLuseXSLT30: length XSLTdata 4 transform is NOT null (OR  < {}), xslt30Processor/xslt30Transformer == null", QueueId, XMLchars.EmptyXSLT_Result.length());
            return XMLchars.EmptyXSLT_Result;
        }
        if ( (checkXSLTtext == null) || ( checkXSLTtext.length() < XMLchars.EmptyXSLT_Result.length() )  ) {
            ConvXMLuseXSLTerr = " ConvXMLuseXSLT30: length XSLTdata 4 transform is null OR  < " + XMLchars.EmptyXSLT_Result.length();
            if ( IsDebugged )
                MessageSend_Log.info("[{}] ConvXMLuseXSLT30: length XSLTdata 4 transform is null OR  < {}", QueueId, XMLchars.EmptyXSLT_Result.length());
            return XMLchars.EmptyXSLT_Result ;
        }
        String serializer_Property_METHOD;
        if(checkXSLTtext.contains("method=\"text\"")) {
            serializer_Property_METHOD =  "text";
        }
        else serializer_Property_METHOD =  "xml";
        ;

        ByteArrayInputStream xmlInputStream=null;
        //BufferedInputStream  _xmlInputStream;
        ByteArrayOutputStream outputByteArrayStream =new ByteArrayOutputStream();
        String stringResult_of_XSLT =XMLchars.EmptyXSLT_Result;
        ConvXMLuseXSLTerr="";
        try {
            xmlInputStream  = new ByteArrayInputStream( XMLdata_4_Tranform.getBytes(StandardCharsets.UTF_8) );
        }
        catch ( Exception exp ) {
            ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
            exp.printStackTrace();
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT30.ByteArrayInputStream Exception" );
            MessageSend_Log.error("[{}] Exception: {}", QueueId, ConvXMLuseXSLTerr);
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT30:");  MsgResult.append( ConvXMLuseXSLTerr );
            return XMLchars.EmptyXSLT_Result ;
        }

        xmlStreamSource = new StreamSource(xmlInputStream);
        try
        {
            if (IsDebugged)
                MessageSend_Log.warn("[{}] ConvXMLuseXSLT30: using XsltLanguageVersion {}, Serializer.Property.METHOD {}", QueueId, xslt30Compiler.getXsltLanguageVersion(), serializer_Property_METHOD);
            Serializer outSerializer = xslt30Processor.newSerializer( outputByteArrayStream );

            outSerializer.setOutputProperty(Serializer.Property.METHOD, serializer_Property_METHOD ); // "xml");
            outSerializer.setOutputProperty(Serializer.Property.ENCODING, "utf-8");
            if ( serializer_Property_METHOD.equals("xml")) {
                outSerializer.setOutputProperty(Serializer.Property.INDENT, "no");
                outSerializer.setOutputProperty(Serializer.Property.OMIT_XML_DECLARATION, "yes");
            }
            SerializationProperties serializationProperties ;
            serializationProperties = outSerializer.getSerializationProperties();
            Properties properties= serializationProperties.getProperties();

            int  serializationPropertiesSise;
            if (IsDebugged)
                MessageSend_Log.warn("[{}] ConvXMLuseXSLT30: Serializer.Property {}, Serializer.Property.all {}", QueueId, properties.size(), properties.toString());

            //for ( serializationPropertiesSise=0; serializationPropertiesSise < properties.size(); serializationPropertiesSise++ ) {     }
            // outSerializer.setOutputStream(outputByteArrayStream);
            xslt30Transformer.transform( xmlStreamSource, outSerializer);

            stringResult_of_XSLT = outputByteArrayStream.toString();
            if (!stringResult_of_XSLT.isEmpty()) {
                // System.err.println("result != null, stringResult_of_XSLT:" + stringResult_of_XSLT );
                if ((stringResult_of_XSLT.charAt(0) == '{') || (stringResult_of_XSLT.charAt(0) == '[')) {
                    if (IsDebugged)
                        MessageSend_Log.warn("[{}] json transformer.transform(`{}`)", QueueId, stringResult_of_XSLT);
                } else if (stringResult_of_XSLT.length() < XMLchars.EmptyXSLT_Result.length()) {
                    ConvXMLuseXSLTerr = " length Xtransformer.transform(`" + stringResult_of_XSLT + "`) < " + XMLchars.EmptyXSLT_Result.length();
                    if (IsDebugged)
                        MessageSend_Log.warn("[{}] length transformer.transform(`{}`) < {}", QueueId, stringResult_of_XSLT, XMLchars.EmptyXSLT_Result.length());
                    stringResult_of_XSLT = XMLchars.EmptyXSLT_Result;
                }
            }
            else {
                ConvXMLuseXSLTerr = " length Xtransformer.transform(`" + stringResult_of_XSLT + "`) == 0 ";
                if (IsDebugged)
                    MessageSend_Log.warn("[{}] length transformer.transform(`{}`) == 0", QueueId, stringResult_of_XSLT);
                stringResult_of_XSLT = XMLchars.EmptyXSLT_Result;
            }


            /*
                if ( IsDebugged ) {
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML IN ): " + XMLdata_4_Tranform);
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessageSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML out ): " + stringResult_of_XSLT);
            }
            */
        }
        catch ( SaxonApiException exp ) {
            ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.Transformer TransformerException" );
            exp.printStackTrace();
            MessageSend_Log.error("[{}] ConvXMLuseXSLT.Transformer TransformerException: {}", QueueId, ConvXMLuseXSLTerr);
            if (  !IsDebugged ) {
                MessageSend_Log.error("[{}] ConvXMLuseXSLT( XML IN ): {}", QueueId, XMLdata_4_Tranform);
                MessageSend_Log.error("[{}] ConvXMLuseXSLT( XSLT ): {}", QueueId, checkXSLTtext);
                MessageSend_Log.error("[{}] ConvXMLuseXSLT( XML out ): {}", QueueId, stringResult_of_XSLT);
            }
            MessageSend_Log.error("[{}] Transformer.Exception: {}", QueueId, ConvXMLuseXSLTerr);
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT.Transformer:");  MsgResult.append( ConvXMLuseXSLTerr );
            throw exp;
            // return XMLchars.EmptyXSLT_Result ;
        }
        return(stringResult_of_XSLT);
    }
}
