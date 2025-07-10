package net.plumbing.msgbus.threads.utils;

import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.threads.TheadDataAccess;
import org.apache.commons.lang3.StringEscapeUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.Attribute;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.slf4j.Logger;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.model.MessageDetailVO;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageDirections;
import net.plumbing.msgbus.model.MessageQueueVO;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
//import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

//import static net.plumbing.msgbus.threads.MessageSendTask.MessegeSend_Log;

public class MessageUtils {

    // static  Long Tag_Num = new  Long(0L);
    public static String MakeEntryOutHeader(@NotNull MessageQueueVO messageQueueVO, int MsgDirectionVO_Key) {
        return ("<" + XMLchars.TagEntryRec + ">"+
                "<" + XMLchars.TagEntryInit + ">" + XMLchars.HermesMsgDirection_Cod + "</" + XMLchars.TagEntryInit + ">" +
                "<" + XMLchars.TagEntryKey + ">" + messageQueueVO.getQueue_Id() + "</" + XMLchars.TagEntryKey + ">"+
                "<" + XMLchars.TagEntrySrc + ">" + XMLchars.HermesMsgDirection_Cod + "</" + XMLchars.TagEntrySrc + ">"+
                "<" + XMLchars.TagEntryDst + ">" + MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getMsgDirection_Cod() + "</" + XMLchars.TagEntryDst + ">" +
                "<" + XMLchars.TagEntryOpId + ">" + messageQueueVO.getOperation_Id() + "</" + XMLchars.TagEntryOpId + ">" +
                "<" + XMLchars.TagOutIdKey + ">" + messageQueueVO.getOutQueue_Id() + "</" + XMLchars.TagOutIdKey + ">" +
                "<" + XMLchars.TagObjectKey + ">" + messageQueueVO.getPerform_Object_Id() + "</" + XMLchars.TagObjectKey + ">" +
                "</" + XMLchars.TagEntryRec + ">"
        );
    }
    public static Long MakeNewMessage_Queue(MessageQueueVO messageQueueVO, TheadDataAccess theadDataAccess, Logger MessegeReceive_Log ){
        ResultSet rs = null;
        try {
            rs = theadDataAccess.stmt_New_Queue_Prepare.executeQuery();
            while (rs.next()) {
                messageQueueVO.setMessageQueue(
                        rs.getLong("Queue_Id"),
                        rs.getTimestamp("Queue_Date"),
                        rs.getString("OutQueue_Id"),
                        rs.getTimestamp("Msg_Date"),
                        rs.getInt("Msg_Status"),
                        rs.getInt("MsgDirection_Id"),
                        rs.getInt("Msg_InfoStreamId"),
                        rs.getInt("Operation_Id"),
                        rs.getString("Queue_Direction"),
                        rs.getString("Msg_Type"),
                        rs.getString("Msg_Reason"),
                        rs.getString("Msg_Type_own"),
                        rs.getString("Msg_Result"),
                        rs.getString("SubSys_Cod"),
                        rs.getString("Prev_Queue_Direction"),
                        rs.getInt("Retry_Count"),
                        rs.getTimestamp("Prev_Msg_Date"),
                        rs.getTimestamp("Queue_Create_Date"),
                        0L
                );
            }
            rs.close();
            theadDataAccess.Hermes_Connection.commit();
        }
        catch (SQLException e)
        {
            MessegeReceive_Log.error(e.getMessage());
            e.printStackTrace();
            MessegeReceive_Log.error( "что то пошло совсем не так...:" + theadDataAccess.selectMessageStatement);
            //if ( rs !=null ) rs.close();
            return null;
        }

        long Queue_Id = messageQueueVO.getQueue_Id();
        try {
            // "(QUEUE_ID, QUEUE_DIRECTION, QUEUE_DATE, MSG_STATUS, MSG_DATE, OPERATION_ID, OUTQUEUE_ID, MSG_TYPE) "
            theadDataAccess.stmt_New_Queue_Insert.setLong(1, Queue_Id);
            theadDataAccess.stmt_New_Queue_Insert.executeUpdate();
            // MessegeReceive_Log.info(  ">" + theadDataAccess.INSERT_Message_Queue + ":Queue_Id=[" + Queue_Id + "] done");

        } catch (SQLException e) {
            MessegeReceive_Log.error("[{}] INSERT_Message_Queue `{}` :{}", Queue_Id, theadDataAccess.INSERT_Message_Queue, sStackTrace.strInterruptedException(e));

            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeReceive_Log.error("[{} ]Hermes_Connection.rollback()fault: {}", Queue_Id, exp.getMessage());
            }
            return null;
        }
        try {
            theadDataAccess.Hermes_Connection.commit();
        } catch (SQLException exp) {
            MessegeReceive_Log.error("Hermes_Connection.commit() fault: {}", exp.getMessage());
            return null;
        }
        return Queue_Id;
    }


    public static Integer ProcessingSendError(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess,
                                              String whyIsFault , boolean isMessageQueue_Directio_2_ErrorOUT, Exception e , Logger MessageSend_Log)
    {
        String ErrorExceptionMessage;
        if ( e != null ) {
            ErrorExceptionMessage = sStackTrace.strInterruptedException(e);
        }
        else ErrorExceptionMessage = ";";

        int messageRetry_Count = messageQueueVO.getRetry_Count();
        messageRetry_Count += 1; // увеличили счетчик попыток

        MessageSend_Log.warn("[{}]ProcessingSendError.messageRetry_Count = {}; getShortRetryCount={}; (getShortRetryCount+getLongRetryCount)={}", messageQueueVO.getQueue_Id(),
                            messageRetry_Count, messageDetails.MessageTemplate4Perform.getShortRetryCount(), messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount());

        if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() ) {

            messageQueueVO.setRetry_Count(messageRetry_Count);
            // переводим время следующей обработки на  ShortRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
            theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getShortRetryInterval(),
                    "Next attempt after " + messageDetails.MessageTemplate4Perform.getShortRetryInterval() + " sec.," + whyIsFault + "fault: " + ErrorExceptionMessage, 1236,
                    messageRetry_Count, MessageSend_Log
            );
            messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now(ZoneId.of( "Europe/Moscow") ) )   );
            messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
            MessageSend_Log.warn("[{}]ProcessingSendError:ClearBodyResponse=({}) Next attempt after {} sec.,{}fault: {}", messageQueueVO.getQueue_Id(),
                                    messageDetails.XML_ClearBodyResponse.toString(), messageDetails.MessageTemplate4Perform.getShortRetryInterval(), whyIsFault, ErrorExceptionMessage);
            return messageRetry_Count;
        }
        if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount() ) {

            messageQueueVO.setRetry_Count(messageRetry_Count);
            // переводим время следующей обработки на  LongRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
            theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getLongRetryInterval(),
                    "Next attempt after " + messageDetails.MessageTemplate4Perform.getLongRetryInterval() + " sec.," + whyIsFault + "fault: " + ErrorExceptionMessage, 1237,
                    messageRetry_Count, MessageSend_Log
            );
            messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now(ZoneId.of( "Europe/Moscow") ) )   );
            messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
            MessageSend_Log.warn("[{}]ProcessingSendError:ClearBodyResponse=({}) Next attempt after {} sec.,{}fault: {}", messageQueueVO.getQueue_Id(),
                                 messageDetails.XML_ClearBodyResponse.toString(), messageDetails.MessageTemplate4Perform.getLongRetryInterval(), whyIsFault, ErrorExceptionMessage);
            return messageRetry_Count;
        }
        if (( isMessageQueue_Directio_2_ErrorOUT ) // Если это не Транспортная ошибка( выставили признак ERROROUT)
            || ( messageRetry_Count >= messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount() )
          )
        {  // не увеличивае счётчик повторов, выставляем ERROUT
            theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                    whyIsFault + " fault: " + ErrorExceptionMessage, 1239,
                     messageQueueVO.getRetry_Count(), MessageSend_Log);
            messageQueueVO.setQueue_Direction(XMLchars.DirectERROUT);
            MessageSend_Log.warn("[{}]ProcessingSendError:ClearBodyResponse=({}) set Queue_Direction == ERROUT,{}fault: {}",
                                   messageQueueVO.getQueue_Id(), messageDetails.XML_ClearBodyResponse.toString(), whyIsFault, ErrorExceptionMessage);
        }
        return messageRetry_Count;
    }

    public static Integer ProcessingOut2ErrorOUT(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess,
                                                 String whyIsFault , Exception e , Logger MessegeSend_Log)
    {
        String ErrorExceptionMessage;
        if ( e != null ) {
            ErrorExceptionMessage = sStackTrace.strInterruptedException(e);
        }
        else ErrorExceptionMessage = ";";

        int result = theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO,
                whyIsFault + " fault: " + ErrorExceptionMessage,  MessegeSend_Log);

        return result;
    }
    public static Integer ProcessingOutError(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess,
                                             String whyIsFault , Exception e , Logger MessegeSend_Log)
    {
        String ErrorExceptionMessage;
        if ( e != null ) {
            ErrorExceptionMessage = sStackTrace.strInterruptedException(e);
        }
        else ErrorExceptionMessage = ";";


        int messageRetry_Count = messageQueueVO.getRetry_Count();
        messageRetry_Count += 1; // увеличили счетчик попыток
        if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() ) {

            messageQueueVO.setRetry_Count(messageRetry_Count);
            messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
            messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
            messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());
            messageQueueVO.setMsg_Reason(whyIsFault + " fault: " + ErrorExceptionMessage);
            // переводим время следующей обработки на  ShortRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
            theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getShortRetryInterval(),
                    "Next attempt after " + messageDetails.MessageTemplate4Perform.getShortRetryInterval() + " sec.," + whyIsFault + "fault: " + ErrorExceptionMessage, 1236,
                    messageRetry_Count, MessegeSend_Log
            );

            return -1;
        }
        if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount() ) {

            messageQueueVO.setRetry_Count(messageRetry_Count);
            messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
            messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
            messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());
            messageQueueVO.setMsg_Reason(whyIsFault + " fault: " + ErrorExceptionMessage);
            // переводим время следующей обработки на  LongRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
            theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getLongRetryInterval(),
                    "Next attempt after " + messageDetails.MessageTemplate4Perform.getLongRetryInterval() + " sec.," + whyIsFault + "fault: " + ErrorExceptionMessage, 1237,
                    messageRetry_Count, MessegeSend_Log
            );
            return -1;
        }
        int result = theadDataAccess.doUPDATE_MessageQueue_Out2ErrorOUT(messageQueueVO,
                whyIsFault + " fault: " + ErrorExceptionMessage,  MessegeSend_Log);

        return result;
    }

    public static String PrepareEnvelope4XSLTPost( MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, Logger MessegeSend_Log) {
        // рассчитываем размер SoapEnvelope
        int SoapEnvelopeSize ;
        String MsgId_Vaue = "<MsgId>" + messageQueueVO.getQueue_Id() + "</MsgId>";
        SoapEnvelopeSize=(XMLchars.Envelope_noNS_Begin.length() + XMLchars.Header_noNS_Begin.length() + MsgId_Vaue.length()+ XMLchars.Header_noNS_End.length()  +
                XMLchars.Body_noNS_Begin.length() + messageDetails.XML_MsgRESOUT.length() + XMLchars.Body_noNS_End.length() + XMLchars.Envelope_noNS_End.length() +16);

        StringBuilder SoapEnvelope = new StringBuilder( SoapEnvelopeSize );
        SoapEnvelope.append(XMLchars.Envelope_noNS_Begin);
        SoapEnvelope.append(XMLchars.Header_noNS_Begin);
        // SoapEnvelope.append("<MsgId>" + messageQueueVO.getQueue_Id() +"</MsgId>");
        SoapEnvelope.append(MsgId_Vaue);
        SoapEnvelope.append(XMLchars.Header_noNS_End);
        SoapEnvelope.append(XMLchars.Body_noNS_Begin);
        SoapEnvelope.append( messageDetails.XML_MsgRESOUT );
        SoapEnvelope.append(XMLchars.Body_noNS_End);
        SoapEnvelope.append(XMLchars.Envelope_noNS_End);

        return SoapEnvelope.toString();
    }

    public static String PrepareEnvelope4ErrTransXSLT( MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, Logger MessegeSend_Log) {
        // рассчитываем размер SoapEnvelope
        String MsgId_Vaue = "<MsgId>" + messageQueueVO.getQueue_Id() + "</MsgId>";
        int SoapEnvelopeSize= (XMLchars.Envelope_noNS_Begin.length() + XMLchars.Header_noNS_Begin.length() + MsgId_Vaue.length()+ XMLchars.Header_noNS_End.length()  +
                XMLchars.Body_noNS_Begin.length() + XMLchars.Body_noNS_End.length() + XMLchars.Envelope_noNS_End.length() +16); ;
        StringBuilder SoapEnvelope = new StringBuilder(SoapEnvelopeSize);
        SoapEnvelope.append(XMLchars.Envelope_noNS_Begin);
        SoapEnvelope.append(XMLchars.Header_noNS_Begin);
        // SoapEnvelope.append("<MsgId>" + messageQueueVO.getQueue_Id() +"</MsgId>");
        SoapEnvelope.append(MsgId_Vaue);
        SoapEnvelope.append(XMLchars.Header_noNS_End);
        SoapEnvelope.append(XMLchars.Body_noNS_Begin);
        SoapEnvelope.append( XMLchars.DirectERROUT );
        SoapEnvelope.append(XMLchars.Body_noNS_End);
        SoapEnvelope.append(XMLchars.Envelope_noNS_End);

        return SoapEnvelope.toString();
    }

    public static String PrepareConfirmation(TheadDataAccess theadDataAccess, MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, boolean isDebugged,Logger MessegeSend_Log) {
        int nn = 0;
        messageDetails.Confirmation.clear();

        String parsedMessageConfirmation = messageDetails.XML_MsgRESOUT.toString();
        String AnswXSLTQueue_Direction = XMLchars.DirectERROUT; // Значение, если вышли с ошибкой
        //AppThead_log.info( parsedConfig );
        if ( parsedMessageConfirmation == null ) {
            theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                    "PrepareConfirmation: результат преобоазования MsgAnswXSLT ( или чистый Response) - пустая строка" + messageDetails.MessageTemplate4Perform.getMsgAnswXSLT(), 1232,
                    messageQueueVO.getRetry_Count(), MessegeSend_Log);
            return AnswXSLTQueue_Direction;
        }
        if (parsedMessageConfirmation.isEmpty()) {
            theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                    "PrepareConfirmation: результат преобоазования MsgAnswXSLT ( или чистый Response) - строка 0-й длины" + messageDetails.MessageTemplate4Perform.getMsgAnswXSLT(), 1232,
                    messageQueueVO.getRetry_Count(), MessegeSend_Log);
            return AnswXSLTQueue_Direction;
        }

        try {
            SAXBuilder documentBuilder = new SAXBuilder();
            //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputStream parsedMessageStream = new ByteArrayInputStream(parsedMessageConfirmation.getBytes(StandardCharsets.UTF_8));
            Document document = (Document) documentBuilder.build(parsedMessageStream); // .parse(parsedConfigStream);

            try {
                String xpathNextExpression;
                String xpathResultCodeExpression;
                String xpathMessageExpression;
                Integer iMsgStaus = 1233;

                XPathExpression<Element> xpathConfirmation = XPathFactory.instance().compile("/Confirmation", Filters.element());
                Element emtConfirmation = xpathConfirmation.evaluateFirst(document);

                if ( emtConfirmation != null ) {
                    xpathNextExpression = "/" + XMLchars.TagConfirmation + "/Next";
                    xpathResultCodeExpression = "/" + XMLchars.TagConfirmation + "/ResultCode";
                    xpathMessageExpression = "/" + XMLchars.TagConfirmation + "/Message";
                } else {
                    xpathNextExpression = "/Result/Next";
                    xpathResultCodeExpression = "/Result/Cod";
                    xpathMessageExpression = "/Result/Text";
                }

                try {
                    XPathExpression<Element> xpathNext = XPathFactory.instance().compile(xpathNextExpression, Filters.element());
                    Element emtNext = xpathNext.evaluateFirst(document);
                    if ( emtNext != null ) {
                        if ( isDebugged )
                        MessegeSend_Log.info("[{}] PrepareConfirmation() XPath has result: <{}> :{}", messageQueueVO.getQueue_Id(), emtNext.getName(), emtNext.getText());
                        AnswXSLTQueue_Direction = emtNext.getText();

                        XPathExpression<Element> xpathResultCode = XPathFactory.instance().compile(xpathResultCodeExpression, Filters.element());
                        Element emtResultCode = xpathResultCode.evaluateFirst(document);
                        if ( emtResultCode != null )
                            try {
                                iMsgStaus = Integer.parseInt(emtResultCode.getText());
                            } catch (NumberFormatException e) {
                                iMsgStaus = 12345;
                                messageDetails.MsgReason.append("Не не получили числового значения Статуса ").append(xpathResultCodeExpression).append(" в результате XSLT преобразования Response");
                            }
                        else {
                            messageDetails.MsgReason.append("Не нашли ").append(xpathResultCodeExpression).append(" в результате XSLT преобразования Response");
                            iMsgStaus = 12346;
                        }

                        XPathExpression<Element> xpathMessage = XPathFactory.instance().compile(xpathMessageExpression, Filters.element());
                        Element emtMessage = xpathMessage.evaluateFirst(document);
                        if ( emtMessage != null )
                            messageDetails.MsgReason.append(emtMessage.getText());
                        else
                            messageDetails.MsgReason.append("Не нашли ").append(xpathMessageExpression).append(" в ").append(parsedMessageConfirmation);

                        if ( emtConfirmation != null ) {

                            // получаем МАХ Tag_Num из messageDetails.Message
                            messageDetails.Message_Tag_Num = 0;
                            for (int i = 0; i < messageDetails.Message.size(); i++) {
                                if ( messageDetails.Message_Tag_Num < messageDetails.Message.get(i).Tag_Num )
                                    messageDetails.Message_Tag_Num = messageDetails.Message.get(i).Tag_Num;
                            }
                            messageDetails.Message_Tag_Num += 1; // Установили Tag_Num для SplitConfirmation
                            messageDetails.Confirmation.clear();
                            if ( isDebugged )
                                MessegeSend_Log.info("[{}] PrepareConfirmation() по записям в количестве Message.size={} получаем МАХ Tag_Num из messageDetails.Message:  :{}", messageQueueVO.getQueue_Id(), messageDetails.Message.size(), messageDetails.Message_Tag_Num );


                            // Split, которая из него сделает набор записей messageDetails.Message -> HashMap<Integer, MessageDetailVO>
                            SplitConfirmation(messageDetails, emtConfirmation, 0, // Tag_Num = messageDetails.Message_Tag_Num !
                                    MessegeSend_Log);

                            // Замещаем полученным массивом messageDetails.Message строки в БД

                            nn = MessageUtils.ReplaceConfirmation(theadDataAccess, messageQueueVO.getQueue_Id(), messageDetails, MessegeSend_Log);
                            if ( nn < 0 ) { //
                                theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                                        "PrepareConfirmation.'" + StringEscapeUtils.unescapeHtml4(messageDetails.MsgReason.toString()) + "' for (" + parsedMessageConfirmation + ")", 1233,
                                        messageQueueVO.getRetry_Count(), MessegeSend_Log);
                                MessegeSend_Log.error("[{}] PrepareConfirmation.'{}' for({})", messageQueueVO.getQueue_Id(), messageDetails.MsgReason.toString(), parsedMessageConfirmation);

                                return XMLchars.DirectERROUT;
                            }
                        }

                        theadDataAccess.doUPDATE_MessageQueue_Send2finishedOUT(messageQueueVO.getQueue_Id(), AnswXSLTQueue_Direction,
                                StringEscapeUtils.unescapeHtml4(messageDetails.MsgReason.toString()),
                                iMsgStaus,
                                messageQueueVO.getRetry_Count(),
                                MessegeSend_Log);

                    } else {
                        theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                                "PrepareConfirmation.XPathFactory.xpath.evaluateFirst('" + xpathNextExpression + "') не нашёл <Next></Next> в (" + parsedMessageConfirmation + ")", 1233,
                                messageQueueVO.getRetry_Count(), MessegeSend_Log);
                        MessegeSend_Log.error("[{}] PrepareConfirmation.XPathFactory.xpath.evaluateFirst('{}') не нашёл <Next></Next> в ({})", messageQueueVO.getQueue_Id(), xpathNextExpression, parsedMessageConfirmation);

                        return AnswXSLTQueue_Direction;
                    }
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                    MessegeSend_Log.error("[{}] PrepareConfirmation.XPathFactory.xpath.evaluateFirst `{}` fault: {} for {}", messageQueueVO.getQueue_Id(), xpathNextExpression, ex.getMessage(), parsedMessageConfirmation);
                    theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                            "PrepareConfirmation.XPathFactory.xpath.evaluateFirst `"+ xpathNextExpression +"` for (" + parsedMessageConfirmation + ") fault: " + ex.getMessage()  , 1233,
                            messageQueueVO.getRetry_Count(), MessegeSend_Log);
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                MessegeSend_Log.error("[{}] PrepareConfirmation.XPathFactory.xpath.evaluateFirst '/Confirmation' fault: {} for {}", messageQueueVO.getQueue_Id(), ex.getMessage(), parsedMessageConfirmation);
                theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                        "PrepareConfirmation.XPathFactory.xpath.evaluateFirst `/Confirmation` for (" + parsedMessageConfirmation + ") fault: " + ex.getMessage()  , 1233,
                        messageQueueVO.getRetry_Count(), MessegeSend_Log);

                return AnswXSLTQueue_Direction;
            }
            // xml-документ в виде строки = messageDetails.XML_MsgSEND поступает
            // Split, которая из него сделает набор записей messageDetails.Message -> HashMap<Integer, MessageDetailVO>


        } catch (JDOMException | IOException ex) {
            MessegeSend_Log.error("PrepareConfirmation.documentBuilder fault: {} for {}", ex.getMessage(), parsedMessageConfirmation);
            theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                    "PrepareConfirmation.documentBuilder fault: " + ex.getMessage() + " for " + parsedMessageConfirmation, 1233,
                    messageQueueVO.getRetry_Count(), MessegeSend_Log);
            return AnswXSLTQueue_Direction;
        }

        return AnswXSLTQueue_Direction;
    }

    public static String PrepareEnvelope4XSLTExt(MessageQueueVO messageQueueVO, String XML_Request_Method, Logger MessegeReceive_Log) {
        // Искуственный Envelope/Head/Body + XML_Request_Method
        int nn = XML_Request_Method.length() +
                2 * (XMLchars.Envelope_noNS_End.length() + XMLchars.Header_noNS_End.length() + XMLchars.MsgId_End.length() + XMLchars.Body_noNS_End.length() )
                + 24;
        StringBuilder SoapEnvelope = new StringBuilder( nn );
        SoapEnvelope.append(XMLchars.Envelope_noNS_Begin);
        SoapEnvelope.append(XMLchars.Header_noNS_Begin);
        SoapEnvelope.append(XMLchars.MsgId_Begin + messageQueueVO.getQueue_Id() + XMLchars.MsgId_End);
        SoapEnvelope.append(XMLchars.Header_noNS_End);

        SoapEnvelope.append(XMLchars.Body_noNS_Begin);
        SoapEnvelope.append( XML_Request_Method );
        SoapEnvelope.append(XMLchars.Body_noNS_End);
        SoapEnvelope.append(XMLchars.Envelope_noNS_End);
        MessegeReceive_Log.warn("Queue_Id=[{}] PrepareEnvelope4XSLTExt: {{}}",messageQueueVO.getQueue_Id(), SoapEnvelope.toString());
        return SoapEnvelope.toString();
    }


    public static int ReadConfirmation(@NotNull TheadDataAccess theadDataAccess, long Queue_Id, @NotNull MessageDetails messageDetails, Logger MessegeReceive_Log) {
        messageDetails.Confirmation.clear();
        messageDetails.ConfirmationRowNum = 0;
        messageDetails.Confirmation_Tag_Num = 0;
        messageDetails.XML_ClearBodyResponse.setLength(0); // XML_MsgConfirmation меняем на XML_ClearBodyResponse
        messageDetails.XML_ClearBodyResponse.trimToSize();
        int Tag_Num=-1;

        try { // получаем Confirmation Tag_Num из select Tag_Num from  Message_QueueDet  WHERE QUEUE_ID = ?Queue_Id and Tag_Par_Num = 0 and tag_Id ='Confirmation'
            theadDataAccess.stmtMsgQueueConfirmationTag.setLong(1, Queue_Id);
            ResultSet rs = theadDataAccess.stmtMsgQueueConfirmationTag.executeQuery();
            while (rs.next()) {
                Tag_Num= rs.getInt("Tag_Num");
            }
            rs.close();
        } catch (SQLException e) {
            MessegeReceive_Log.error("Queue_Id=[{}] ReadConfirmation() select for tag_Id =Confirmation ,  SQLException:{}", Queue_Id, sStackTrace.strInterruptedException(e));
            System.err.println("Queue_Id=[" + Queue_Id + "] :" + e.getMessage() );
            e.printStackTrace();
            return messageDetails.ConfirmationRowNum;
        }
        if ( Tag_Num < 1 ) {
            MessegeReceive_Log.warn("Queue_Id=[{}] ReadConfirmation(): tag 'Confirmation' is not found in MESSAGE_QUEUEDET", Queue_Id);
            messageDetails.XML_ClearBodyResponse.append(XMLchars.OpenTag)
                    .append(XMLchars.TagConfirmation).append(XMLchars.CloseTag)
                    .append(XMLchars.nanXSLT_Result)
                    .append(XMLchars.OpenTag)
                    .append(XMLchars.TagConfirmation).append(XMLchars.EndTag).append(XMLchars.CloseTag);
            messageDetails.ConfirmationRowNum = 0;
            return 0;
        }
        try {
            // зачитываем тег <Confirmation> и все теги, которые под ним
             // select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num from  dbSchema.message_QueueDet D where (1=1) and d.QUEUE_ID = ? and d.Tag_Num = ?
             //               union all
             //  select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num from "+ dbSchema +".message_QueueDet D where (1=1)  and d.QUEUE_ID = ? and d.Tag_Par_Num >= ?
             //         order by   4, 3

            theadDataAccess.stmtMsgQueueConfirmationDet.setLong(1, Queue_Id);
            theadDataAccess.stmtMsgQueueConfirmationDet.setInt(2, Tag_Num);
            theadDataAccess.stmtMsgQueueConfirmationDet.setLong(3, Queue_Id);
            theadDataAccess.stmtMsgQueueConfirmationDet.setInt(4, Tag_Num);
            ResultSet rs = theadDataAccess.stmtMsgQueueConfirmationDet.executeQuery();
            String rTag_Value=null;
            while (rs.next()) {
                MessageDetailVO messageDetailVO = new MessageDetailVO();
                rTag_Value = org.apache.commons.text.StringEscapeUtils.escapeXml10(rs.getString("Tag_Value") );
//                    MessegeReceive_Log.warn("_ReadConfirmation messageChildVO.Tag_Par_Num=" + rs.getInt("Tag_Par_Num") +
//                            ", messageChildVO.Tag_Num=" + rs.getInt("Tag_Num") +
//                            ", messageChildVO.Tag_Id=" + rs.getString("Tag_Id") +
//                            ", messageChildVO.Tag_Value=" + rTag_Value
//                    );

                if ( rTag_Value == null )
                    messageDetailVO.setMessageQueue(
                            rs.getString("Tag_Id"),
                            null,
                            rs.getInt("Tag_Num"),
                            rs.getInt("Tag_Par_Num")
                    );
                else
                    messageDetailVO.setMessageQueue(
                            rs.getString("Tag_Id"),
                            org.apache.commons.text.StringEscapeUtils.escapeXml10(stripNonValidXMLCharacters(rTag_Value)),
                            //StringEscapeUtils.escapeXml10(rTag_Value.replaceAll(XMLchars.XML10pattern,"")),
                            // StringEscapeUtils.escapeXml10(rTag_Value).replaceAll(XMLchars.XML10pattern,""),
                            rs.getInt("Tag_Num"),
                            rs.getInt("Tag_Par_Num")
                    );
                messageDetails.Confirmation.put(messageDetails.ConfirmationRowNum, messageDetailVO);
                messageDetails.ConfirmationRowNum += 1;
                // MessegeReceive_Log.info( "Tag_Id:" + rs.getString("Tag_Id") + " [" + rs.getString("Tag_Value") + "]");
            }
            rs.close();
        } catch (SQLException e) {
            MessegeReceive_Log.error("Queue_Id=[{}] ReadConfirmation() read body of has SQLException:{}", Queue_Id, sStackTrace.strInterruptedException(e));
            e.printStackTrace();
            return -2;
        }
//                if (  messageDetails.MessageTemplate4Perform.getIsDebugged() ) {
//                    for (int i = 0; i < messageDetails.Confirmation.size(); i++) {
//                        MessageDetailVO messageChildVO = messageDetails.Confirmation.get(i);
//                        MessegeReceive_Log.warn("_ReadConfirmation done messageChildVO.Tag_Par_Num=" + messageChildVO.Tag_Par_Num +
//                                ", messageChildVO.Tag_Num=" + messageChildVO.Tag_Num +
//                                ", messageChildVO.Tag_Id=" + messageChildVO.Tag_Id +
//                                ", messageChildVO.Tag_Value=" + messageChildVO.Tag_Value
//                        );
//                    }
//                }
        if ( messageDetails.ConfirmationRowNum > 0 )


            XML_CurrentConfirmation_Tags(messageDetails, 0, MessegeReceive_Log);
        if (  messageDetails.MessageTemplate4Perform.getIsDebugged() )
            MessegeReceive_Log.info("[{}] MsgConfirmation: {}", Queue_Id, messageDetails.XML_ClearBodyResponse.toString());
        return messageDetails.ConfirmationRowNum;
    }

    public static int XML_CurrentConfirmation_Tags(MessageDetails messageDetails, int Current_Elm_Key, Logger MessegeReceive_Log) {
        MessageDetailVO messageDetailVO = messageDetails.Confirmation.get(Current_Elm_Key);
//        boolean is_Local_Debug= true;
        if ( messageDetailVO.Tag_Num != 0 ) {

            // !было:  StringBuilder XML_Tag = new StringBuilder( XMLchars.OpenTag + messageDetailVO.Tag_Id );
            // стало:
            messageDetails.XML_ClearBodyResponse.append(XMLchars.OpenTag).append(messageDetailVO.Tag_Id);
            // XML_Tag.append ( "<" + messageDetailVO.Tag_Id + ">" );
            // цикл по формированию параметров-аьтрибутов элемента
            for (int i = 0; i < messageDetails.Confirmation.size(); i++) {
                MessageDetailVO messageChildVO = messageDetails.Confirmation.get(i);
//                MessegeReceive_Log.warn("_CurrentConfirmation_Tags messageChildVO.Tag_Par_Num=" +  messageChildVO.Tag_Par_Num +
//                                ", messageChildVO.Tag_Num=" + messageChildVO.Tag_Num +
//                                ", messageChildVO.Tag_Id=" + messageChildVO.Tag_Id +
//                                ", messageChildVO.Tag_Value=" + messageChildVO.Tag_Value
//                );
                if ( (messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент
                        (messageChildVO.Tag_Num == 0) )  // это атрибут элемента, у которого нет потомков
                {
                    if ( messageChildVO.Tag_Value != null )
                        // !было:XML_Tag.append ( XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + messageChildVO.Tag_Value + XMLchars.Quote );
                        // стало:
                        messageDetails.XML_ClearBodyResponse.append(XMLchars.Space).append(messageChildVO.Tag_Id).
                                append(XMLchars.Equal).append(XMLchars.Quote).append(messageChildVO.Tag_Value).append(XMLchars.Quote);
                        // по
                        //                messageDetails.XML_MsgConfirmation.append(XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + messageChildVO.Tag_Value + XMLchars.Quote);
                    else
                        // !было: XML_Tag.append ( XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + XMLchars.Quote );
                        // стало:
                        messageDetails.XML_ClearBodyResponse.append(XMLchars.Space)
                                .append( messageChildVO.Tag_Id)
                                .append( XMLchars.Equal)
                                .append( XMLchars.Quote).append( "noName").append(XMLchars.Quote);
                }
            }
            // !было: XML_Tag.append( XMLchars.CloseTag);
            // стало:
            messageDetails.XML_ClearBodyResponse.append(XMLchars.CloseTag);

            if ( messageDetailVO.Tag_Value != null )
                // !было: XML_Tag.append( messageDetailVO.getTag_Value() );
                // стало:
                messageDetails.XML_ClearBodyResponse.append(messageDetailVO.getTag_Value());

            for (int i = 0; i < messageDetails.Confirmation.size(); i++) {
                MessageDetailVO messageChildVO = messageDetails.Confirmation.get(i);

                if ( (messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент
                        (messageChildVO.Tag_Num != 0) )  // И это элемент, который может быть потомком!
                {
                    // !было: XML_Tag.append ( XML_Current_Tags( messageDetails, i) );
                    // стало:
                    XML_CurrentConfirmation_Tags(messageDetails, i, MessegeReceive_Log );
                }
            }

            // !было: XML_Tag.append( XMLchars.OpenTag + XMLchars.EndTag + messageDetailVO.Tag_Id + XMLchars.CloseTag);
            // стало:
            messageDetails.XML_ClearBodyResponse.append(XMLchars.OpenTag).append(XMLchars.EndTag).append( messageDetailVO.Tag_Id ).append(XMLchars.CloseTag);
            return 1; //XML_Tag;
        } else {
            // !было: return 0;
            // Теряются отрибуты по считывании Confirmation
            if ( messageDetailVO.Tag_Value != null ) {   // !было:XML_Tag.append ( XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + messageChildVO.Tag_Value + XMLchars.Quote );
                // стало:
                messageDetails.XML_ClearBodyResponse.append(XMLchars.Space).append( messageDetailVO.Tag_Id).append( XMLchars.Equal)
                        .append( XMLchars.Quote).append( messageDetailVO.Tag_Value).append( XMLchars.Quote);
                return 1; //XML_Tag;
            }
            else
                return 0; //XML_Tag;
        }
    }



    public static int ReadMessage(TheadDataAccess theadDataAccess, long Queue_Id, @NotNull MessageDetails messageDetails, boolean IsDebugged, Logger MessegeSend_Log) {
        messageDetails.Message.clear();
        messageDetails.MessageRowNum = 0;
        messageDetails.Message_Tag_Num = 0;

        Pattern pattern = Pattern.compile("\\d+");
        int total_Elm_Length=0;
        try {
            theadDataAccess.stmtMsgQueueDet.setLong(1, Queue_Id);
            ResultSet rs = theadDataAccess.stmtMsgQueueDet.executeQuery();
            String rTag_Value=null;
            String rTag_Id=null;
            String xmlTag_Id;
            int PrevTag_Par_Num=-102030405;
            int rTag_Num;
            int rTag_Par_Num;

            int rTag_ValueLength;
            ArrayList<Integer> ChildVO_ArrayList= new ArrayList<Integer>();

            while (rs.next()) {
                MessageDetailVO messageDetailVO = new MessageDetailVO();
                rTag_Value = rs.getString("Tag_Value");
                rTag_Num= rs.getInt("Tag_Num");
                rTag_Par_Num = rs.getInt("Tag_Par_Num");
                rTag_Id= rs.getString("Tag_Id");
                if ( pattern.matcher( rTag_Id ).matches() )
                    xmlTag_Id = "Element" + rTag_Id;
                else
                    xmlTag_Id = rTag_Id;
                if ( rTag_Value == null ) {
                    messageDetailVO.setMessageQueue(xmlTag_Id, null, rTag_Num, rTag_Par_Num);
                    rTag_ValueLength = 0;
                }
                else {
                    messageDetailVO.setMessageQueue(xmlTag_Id,
                            StringEscapeUtils.escapeXml10(stripNonValidXMLCharacters(rTag_Value)),
                            rTag_Num, rTag_Par_Num);
                    rTag_ValueLength = messageDetailVO.getTag_Value().length();
                }
                total_Elm_Length = total_Elm_Length + rTag_Id.length() * 2  + rTag_ValueLength + 24;

                messageDetails.Message.put(messageDetails.MessageRowNum, messageDetailVO);

                // для получения messageDetails.Message.get( messageDetails.MessageIndex_by_Tag_Par_Num.get(Tag_Par_Num) )
                if (PrevTag_Par_Num != rTag_Par_Num) { // получили элемент у которого другой "пара"
                    if (PrevTag_Par_Num != -102030405 ) {
                        //MessegeSend_Log.warn("PrevTag_Par_Num[" + PrevTag_Par_Num +"]: ChildVO_ArrayList size=" + ChildVO_ArrayList.size());
                        ChildVO_ArrayList = new ArrayList<Integer>();
                    }

                    messageDetails.MessageIndex_by_Tag_Par_Num.put( rTag_Par_Num, ChildVO_ArrayList);
                    PrevTag_Par_Num = rTag_Par_Num;
                }
                else {
                    ChildVO_ArrayList = messageDetails.MessageIndex_by_Tag_Par_Num.get(rTag_Par_Num);
                    //MessegeSend_Log.warn("rTag_Par_Num[" + rTag_Par_Num +"]: ChildVO_ArrayList size=" + ChildVO_ArrayList.size());
                }
                ChildVO_ArrayList.add( messageDetails.MessageRowNum );
                //MessegeSend_Log.warn("add to Tag_Par_Num[" + rTag_Par_Num +"]: ChildVO_ArrayList size=" + ChildVO_ArrayList.size());

                messageDetails.MessageRowNum += 1;
                if ( messageDetails.MessageRowNum % 10000 == 0)
                    MessegeSend_Log.info("[{}] читаем из БД тело XML, {} записей", Queue_Id, messageDetails.MessageRowNum);
                // MessegeSend_Log.info( "Tag_Id:" + rs.getString("Tag_Id") + " [" + rs.getString("Tag_Value") + "]");
            }
        } catch (SQLException e) {
            MessegeSend_Log.error("Queue_Id=[{}] :{}", Queue_Id, sStackTrace.strInterruptedException(e));
            e.printStackTrace();
            return messageDetails.MessageRowNum;
        }
        MessegeSend_Log.info("[{}] считали из БД фрагменты XML, {} записей, предполагаемый объём XML {} символов", Queue_Id, messageDetails.MessageRowNum, total_Elm_Length);

        if ( messageDetails.MessageRowNum > 0 )
            try {
                messageDetails.XML_MsgOUT = null;
                messageDetails.XML_MsgOUT = new StringBuilder( total_Elm_Length );
                XML_Current_Tags( messageDetails, 0);
            } catch ( NullPointerException e ) {
                // NPE случилось, печатаем диагностику
                MessegeSend_Log.warn("[{}] проверяем вторчный индекс MessageIndex_by_Tag_Par_Num, потому как получили NullPointerException на подготовке XML", Queue_Id);
                Set<Integer> MessageIndexSet = messageDetails.MessageIndex_by_Tag_Par_Num.keySet();
                Iterator MessageIndexIterator = MessageIndexSet.iterator();
                while (MessageIndexIterator.hasNext()) {
                    Integer i = (Integer) MessageIndexIterator.next();
                    MessegeSend_Log.warn("[{}] MessageIndex_by_Tag_Par_Num[{}]{}", Queue_Id, i, messageDetails.MessageIndex_by_Tag_Par_Num.get(i).toString());
                }
                MessageIndexSet = messageDetails.Message.keySet();
                Iterator messageDetailsIterator = MessageIndexSet.iterator();
                while (messageDetailsIterator.hasNext()) {
                    Integer i = (Integer) messageDetailsIterator.next();
                    MessegeSend_Log.warn("[{}] messageDetails.Message[{}] <{}>{}; Tag_Num={}; Tag_Par_Num={}", Queue_Id, i,
                                                messageDetails.Message.get(i).Tag_Id, messageDetails.Message.get(i).Tag_Value, messageDetails.Message.get(i).Tag_Num, messageDetails.Message.get(i).Tag_Par_Num);
                }
                MessegeSend_Log.info("[{}] тело XML тело XML не получено из БД, остановлено на {} символов", Queue_Id, messageDetails.XML_MsgOUT.length());
            }

        MessegeSend_Log.info("[{}] получили из БД тело XML, {} символов", Queue_Id, messageDetails.XML_MsgOUT.length());
        if (IsDebugged ) {
            MessegeSend_Log.info(messageDetails.XML_MsgOUT.toString());
        }
        return messageDetails.MessageRowNum;
    }

    public static String stripNonValidXMLCharacters(String in) {
        StringBuffer out = new StringBuffer(); // Used to hold the output.
        char current; // Used to reference the current character.

        if (in == null || ("".equals(in))) return ""; // vacancy test.
        for (int i = 0; i < in.length(); i++) {
            current = in.charAt(i); // NOTE: No IndexOutOfBoundsException caught here; it should not happen.
            if ((current == 0x9) ||
                    (current == 0xA) ||
                    (current == 0xD) ||
                    ((current >= 0x20) && (current <= 0xD7FF)) ||
                    ((current >= 0xE000) && (current <= 0xFFFD)) ||
                    ((current >= 0x10000) && (current <= 0x10FFFF)))
                out.append(current);
        }
        return out.toString();
    }

    // @messageDetails.XML_MsgOUT формируется из messageDetails.Message
    public static int XML_Current_Tags(@NotNull MessageDetails messageDetails, int Current_Elm_Key) throws UnsupportedOperationException, NullPointerException  {
        MessageDetailVO messageDetailVO = messageDetails.Message.get(Current_Elm_Key);


        if ( messageDetailVO.Tag_Num != 0 ) { // Tag_Num Всегда начинается с 1 для сообщения! ( проверка на всякий случай )

            messageDetails.XML_MsgOUT.append(XMLchars.OpenTag ); messageDetails.XML_MsgOUT.append( messageDetailVO.Tag_Id);

            MessageDetailVO messageChildVO;
            ArrayList<Integer> ChildVO_ArrayList =  messageDetails.MessageIndex_by_Tag_Par_Num.get( messageDetailVO.Tag_Num );
            if (ChildVO_ArrayList != null ) { // у элемента <messageDetailVO.Tag_Id ...> есть либо атрибуты, либо вложенные элементы
                Iterator<Integer> ChildVO_AttributeIterator = ChildVO_ArrayList.iterator();
                    // 1й проход, достаём атрибуты элемента
                    while (ChildVO_AttributeIterator.hasNext()) {
                        Integer i = ChildVO_AttributeIterator.next();
                        messageChildVO = messageDetails.Message.get(i);
                        if ( messageChildVO != null) {
                            if ((messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент
                                    (messageChildVO.Tag_Num == 0))  // это атрибут элемента, у которого нет потомков
                            {
                                if (messageChildVO.Tag_Value != null) { // фармирукм Attribute="Value"
                                    messageDetails.XML_MsgOUT.append(XMLchars.Space);
                                    messageDetails.XML_MsgOUT.append(messageChildVO.Tag_Id);
                                    messageDetails.XML_MsgOUT.append(XMLchars.Equal);
                                    messageDetails.XML_MsgOUT.append(XMLchars.Quote);
                                    messageDetails.XML_MsgOUT.append(messageChildVO.Tag_Value);
                                    messageDetails.XML_MsgOUT.append(XMLchars.Quote);
                                } else {  // фармирукм Attribute=""
                                    messageDetails.XML_MsgOUT.append(XMLchars.Space);
                                    messageDetails.XML_MsgOUT.append(messageChildVO.Tag_Id);
                                    messageDetails.XML_MsgOUT.append(XMLchars.Equal);
                                    messageDetails.XML_MsgOUT.append(XMLchars.Quote);
                                    messageDetails.XML_MsgOUT.append(XMLchars.Quote);
                                }
                            }
                        }
                        else {
                           String messageException = "1й проход, достаём атрибуты элемента: XML_Current_Tags [" + Current_Elm_Key + "] messageDetails.Message.get[" + i + "] вернуло NULL! Tag_Id: <" +
                                   messageDetails.Message.get(Current_Elm_Key).Tag_Id + "> Tag_Value=" +
                                   messageDetails.Message.get(Current_Elm_Key).Tag_Value +
                                   "; Tag_Num=" + messageDetails.Message.get(Current_Elm_Key).Tag_Num +
                                   "; Tag_Par_Num=" + messageDetails.Message.get(Current_Elm_Key).Tag_Par_Num;
                         //  MessegeSend_Log.error(messageException);
                           throw new NullPointerException( messageException);
                        }
                    }
                messageDetails.XML_MsgOUT.append(XMLchars.CloseTag); // + ">" );
                if ( messageDetailVO.Tag_Value != null )
                    messageDetails.XML_MsgOUT.append(messageDetailVO.getTag_Value());

                Iterator<Integer> ChildVO_ElementIterator = ChildVO_ArrayList.iterator();
                // 2й проход, достаём дочерние элементы
                while (ChildVO_ElementIterator.hasNext()) {
                    Integer i = ChildVO_ElementIterator.next();
                    messageChildVO = messageDetails.Message.get(i);
                    if (messageChildVO != null) {
                        if ((messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент - проверка!!!
                                (messageChildVO.Tag_Num != 0))  // И это элемент, который может быть потомком!
                        {  // вызываем рекурсию
                            XML_Current_Tags(messageDetails, i);
                        }
                    }
                    else {
                        String messageException = "2й проход, достаём дочерние элементы: XML_Current_Tags [" + Current_Elm_Key + "] messageDetails.Message.get[" + i + "] вернуло NULL! Tag_Id: <" +
                                messageDetails.Message.get(Current_Elm_Key).Tag_Id + "> Tag_Value=" +
                                messageDetails.Message.get(Current_Elm_Key).Tag_Value +
                                "; Tag_Num=" + messageDetails.Message.get(Current_Elm_Key).Tag_Num +
                                "; Tag_Par_Num=" + messageDetails.Message.get(Current_Elm_Key).Tag_Par_Num;
                        // MessegeSend_Log.error(messageException);
                        throw new NullPointerException( messageException);
                    }
                }

            } else { // вложенных элементов нет, чисто значение
                messageDetails.XML_MsgOUT.append(XMLchars.CloseTag); // + ">" );
                if ( messageDetailVO.Tag_Value != null )
                    messageDetails.XML_MsgOUT.append(messageDetailVO.getTag_Value());
            }

            messageDetails.XML_MsgOUT.append(XMLchars.OpenTag ); // <\Tag_Id>
            messageDetails.XML_MsgOUT.append( XMLchars.EndTag );
            messageDetails.XML_MsgOUT.append( messageDetailVO.Tag_Id );
            messageDetails.XML_MsgOUT.append( XMLchars.CloseTag);
            return 1; //XML_Tag;
        } else {
            // !было: StringBuilder XML_Tag = new StringBuilder(XMLchars.Space);
            return 0; //XML_Tag;
        }
    }

    public static int _4_save_XML_Current_Tags(@NotNull MessageDetails messageDetails, int Current_Elm_Key) {
        MessageDetailVO messageDetailVO = messageDetails.Message.get(Current_Elm_Key);

        if ( messageDetailVO.Tag_Num != 0 ) { // Tag_Num Всегда начинается с 1 для сообщения! ( проверка на всякий случай )
            MessageDetailVO messageChildVO;

            messageDetails.XML_MsgOUT.append(XMLchars.OpenTag ); messageDetails.XML_MsgOUT.append( messageDetailVO.Tag_Id);
            // XML_Tag.append ( "<" + messageDetailVO.Tag_Id + ...
            // цикл по формированию параметров-аьтрибутов элемента
            for (int i = 0; i < messageDetails.Message.size(); i++) {
                // MessageDetailVO
                messageChildVO = messageDetails.Message.get(i);
                if ( (messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент
                        (messageChildVO.Tag_Num == 0) )  // это атрибут элемента, у которого нет потомков
                {
                    if ( messageChildVO.Tag_Value != null ){ // фармирукм Attribute="Value"
                        messageDetails.XML_MsgOUT.append( XMLchars.Space );
                        messageDetails.XML_MsgOUT.append( messageChildVO.Tag_Id );
                        messageDetails.XML_MsgOUT.append( XMLchars.Equal );
                        messageDetails.XML_MsgOUT.append( XMLchars.Quote);
                        messageDetails.XML_MsgOUT.append( messageChildVO.Tag_Value );
                        messageDetails.XML_MsgOUT.append( XMLchars.Quote);
                    }
                    else {  // фармирукм Attribute=""
                        messageDetails.XML_MsgOUT.append(XMLchars.Space );
                        messageDetails.XML_MsgOUT.append( messageChildVO.Tag_Id);
                        messageDetails.XML_MsgOUT.append( XMLchars.Equal);
                        messageDetails.XML_MsgOUT.append( XMLchars.Quote );
                        messageDetails.XML_MsgOUT.append( XMLchars.Quote);
                    }
                }
            }
            messageDetails.XML_MsgOUT.append(XMLchars.CloseTag); // + ">" );

            if ( messageDetailVO.Tag_Value != null )
                messageDetails.XML_MsgOUT.append(messageDetailVO.getTag_Value());

            for (int i = 0; i < messageDetails.Message.size(); i++) {
                // MessageDetailVO
                messageChildVO = messageDetails.Message.get(i);
                if ( (messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент
                        (messageChildVO.Tag_Num != 0) )  // И это элемент, который может быть потомком!
                {
                    // !было: XML_Tag.append ( XML_Current_Tags( messageDetails, i) );
                    // стало:
                    XML_Current_Tags(messageDetails, i);
                }
            }

            messageDetails.XML_MsgOUT.append(XMLchars.OpenTag ); // <\Tag_Id>
            messageDetails.XML_MsgOUT.append( XMLchars.EndTag );
            messageDetails.XML_MsgOUT.append( messageDetailVO.Tag_Id );
            messageDetails.XML_MsgOUT.append( XMLchars.CloseTag);
            return 1; //XML_Tag;
        } else {
            // !было: StringBuilder XML_Tag = new StringBuilder(XMLchars.Space);
            return 0; //XML_Tag;
        }
    }

    public static int ReplaceMessage4SEND(TheadDataAccess theadDataAccess, long Queue_Id, @NotNull MessageDetails messageDetails, MessageQueueVO messageQueueVO , Logger MessegeSend_Log) {
        int nn = 0;
        // Надо переносить переинициализацию messageDetails.Message после того, как распарселили новый XML
        // messageDetails.Message.clear();
        // messageDetails.MessageRowNum = 0;
        // а тут закоментарили !
        String parsedMessage4SEND = messageDetails.XML_MsgSEND;
        //AppThead_log.info( parsedConfig );
        if ( parsedMessage4SEND == null ) return -2;
        if ( parsedMessage4SEND.length() == 0 ) return -3;

        try {
            SAXBuilder documentBuilder = new SAXBuilder();
            //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputStream parsedMessageStream = new ByteArrayInputStream(parsedMessage4SEND.getBytes(StandardCharsets.UTF_8));
            Document document = (Document) documentBuilder.build(parsedMessageStream); // .parse(parsedConfigStream);

            Element RootElement = document.getRootElement();
            // HE-5864 Спец.символ UTF-16 или любой другой invalid XML character
            // Надо переносить пепеинициализацию messageDetails.Message после того, как распарселили новый XML
            //int Tag_Par_Num = 0;
            messageDetails.Message_Tag_Num = 0;
            messageDetails.Message.clear();
            messageDetails.MessageRowNum = 0;

            // xml-документ в виде строки = messageDetails.XML_MsgSEND поступает
            // Split, которая из него сделает набор записей messageDetails.Message -> HashMap<Integer, MessageDetailVO>
            SplitMessage(messageDetails, RootElement, 0, // Tag_Num = messageDetails.Message_Tag_Num !
                    MessegeSend_Log);

        } catch (JDOMException | IOException ex) {
            ex.printStackTrace(System.out);
            MessegeSend_Log.error("[{}] ReplaceMessage4SEND fault:{}", Queue_Id, ex.getMessage());
            MessageUtils.ProcessingOut2ErrorOUT(  messageQueueVO, messageDetails,  theadDataAccess,
                    "ReplaceMessage4SEND.SAXBuilder fault:"  + ex.getMessage() + " " + messageDetails.XML_MsgSEND   ,
                    null ,  MessegeSend_Log);
            return -22; // HE-5864 Спец.символ UTF-16 или любой другой invalid XML character
        }

        // Замещаем полученным массивом messageDetails.Message строки в БД
        nn = MessageUtils.ReplaceMessage(theadDataAccess, Queue_Id, messageDetails, MessegeSend_Log);

        return nn;
    }

    public static int SplitMessage(MessageDetails messageDetails, Element EntryElement, int tag_Par_Num,
                                   Logger MessegeSend_Log) {
        // Tag_Par_Num- №№ Тага, к которому прилепляем всё от EntryElement,ссылка на Tag_Num родителя
        // Tag_Num - сквозной!!! нумератор записей

        int nn = 0;
        //MessegeSend_Log.info("Split[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + EntryElement.getName() + ">");

        if ( EntryElement.isRootElement() ) {
            // Tag_Num += 1;
            String ElementPrefix = EntryElement.getNamespacePrefix();
            String ElementEntry;
            if ( ElementPrefix.length() > 0 ) {
                ElementEntry = ElementPrefix + ":" + EntryElement.getName();
            } else
                ElementEntry = EntryElement.getName();


            //MessegeSend_Log.info("Tag_Par_Num[0][1]: <" + ElementEntry + ">");
            MessageDetailVO messageDetailVO = new MessageDetailVO();
            messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                    "", // Tag_Value
                    1,
                    0
            );
            messageDetails.Message.put(messageDetails.MessageRowNum, messageDetailVO);
            messageDetails.MessageRowNum += 1;
            List<Namespace> ElementNamespaces = EntryElement.getNamespacesIntroduced();
            for (int j = 0; j < ElementNamespaces.size(); j++) {
                // Namespace не увеличивает Tag_Num ( сквозной нумератор записей )
                // в БД имеет Tag_Num= 0, ссылается на элемент.
                Namespace Namespace = ElementNamespaces.get(j);

                //MessegeSend_Log.info("Tag_Par_Num[1][0]: " + XMLchars.XMLns + Namespace.getPrefix() + "=" + Namespace.getURI());
                MessageDetailVO NSmessageDetailVO = new MessageDetailVO();
                NSmessageDetailVO.setMessageQueue(XMLchars.XMLns + Namespace.getPrefix(), // "Tag_Id"
                        Namespace.getURI(), // Tag_Value
                        0,
                        1
                );
                messageDetails.Message.put(messageDetails.MessageRowNum, NSmessageDetailVO);
                messageDetails.MessageRowNum += 1;

            }
            // после заполнения данных для корневого элемента, для всех его детей нужен  Tag_Par_Num== 1 !
            tag_Par_Num = 1;
            messageDetails.Message_Tag_Num += 1;
        }

        String ElementPrefix;
        List<Element> Elements = EntryElement.getChildren();
        // Перебор всех элементов TemplConfig
        for (int i = 0; i < Elements.size(); i++) {
            Element XMLelement = (Element) Elements.get(i);

            ElementPrefix = XMLelement.getNamespacePrefix();
            String ElementEntry;
            if ( ElementPrefix.length() > 0 ) {
                ElementEntry = ElementPrefix + ":" + XMLelement.getName();
            } else
                ElementEntry = XMLelement.getName();

            String ElementContent = XMLelement.getText();
            MessageDetailVO messageDetailVO = new MessageDetailVO();

            messageDetails.Message_Tag_Num += 1;

            if ( ElementContent.length() > 0 ) {
                //MessegeSend_Log.info("Tag_Par_Num[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + ElementEntry + ">=" + ElementContent);
                messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                        ElementContent, // Tag_Value
                        messageDetails.Message_Tag_Num,
                        tag_Par_Num
                );
            } else {
                //MessegeSend_Log.info("Tag_Par_Num[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + ElementEntry + ">");

                messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                        "", // Tag_Value
                        messageDetails.Message_Tag_Num,
                        tag_Par_Num // Tag_Num += 1; будет сделано в Tag_Par_Num
                );
            }
            messageDetails.Message.put(messageDetails.MessageRowNum, messageDetailVO);
            messageDetails.MessageRowNum += 1;
            String AttributePrefix;
            String AttributeEntry;

            List<Attribute> ElementAttributes = XMLelement.getAttributes();
            for (int j = 0; j < ElementAttributes.size(); j++) {
                Attribute XMLattribute = ElementAttributes.get(j);
                MessageDetailVO ATTRmessageDetailVO = new MessageDetailVO();
                AttributePrefix = XMLattribute.getNamespacePrefix();
                if ( AttributePrefix.length() > 0 ) {
                    AttributeEntry = AttributePrefix + ":" + XMLattribute.getName();
                } else
                    AttributeEntry = XMLattribute.getName();
                String AttributeValue = XMLattribute.getValue();
                // Attribute не увеличивает Tag_Num ( сквозной нумератор записей )
                // в БД имеет Tag_Num= 0, ссылается на элемент.
                // MessegeSend_Log.info("Tag_Par_Num[" + messageDetails.Message_Tag_Num + "][" + 0 + "]: \"" + AttributeEntry + "\"=" + AttributeValue);
                ATTRmessageDetailVO.setMessageQueue(AttributeEntry, // "Tag_Id"
                        AttributeValue, // Tag_Value
                        0,
                        messageDetails.Message_Tag_Num
                );
                messageDetails.Message.put(messageDetails.MessageRowNum, ATTRmessageDetailVO);
                messageDetails.MessageRowNum += 1;
            }
            // Tag_Par_Num += 1;  /// ??????????????????????????????????? Явно не то.
            // int tag_Par_Num_4_Child = Tag_Num.intValue();
            SplitMessage(messageDetails, XMLelement,
                    messageDetails.Message_Tag_Num, // Tag_Par_Num  для рекурсии
                    // Tag_Num, // Tag_Num += 1; будет сделано в цикле по дочерним элементам перед добавлением
                    MessegeSend_Log);

        }
        return nn;
    }

    public static int ReplaceMessage(@NotNull TheadDataAccess theadDataAccess, long Queue_Id, @NotNull MessageDetails messageDetails, Logger MessegeSend_Log) {
        int nn = 0;

        try {
            theadDataAccess.stmt_DELETE_Message_Details.setLong(1, Queue_Id);
            theadDataAccess.stmt_DELETE_Message_Details.executeUpdate();
        } catch (SQLException e) {
            MessegeSend_Log.error("DELETE({}[{}]) ReplaceMessage fault: {}", theadDataAccess.DELETE_Message_Details, Queue_Id, e.getMessage());
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeSend_Log.error("[{}]Hermes_Connection.rollback()fault: {}" , Queue_Id, exp.getMessage());
            }
            return -1;
        }

        try {
            for (int i = 0; i < messageDetails.Message.size(); i++) {
                MessageDetailVO MessageDetailVO = messageDetails.Message.get(i);
                theadDataAccess.stmt_INSERT_Message_Details.setLong(1, Queue_Id);
                theadDataAccess.stmt_INSERT_Message_Details.setString(2, MessageDetailVO.Tag_Id);
                if ( MessageDetailVO.Tag_Value.length() > (XMLchars.MAX_TAG_VALUE_BYTE_SIZE /2) ) {
                    String ElementContentS = new String( XMLchars.cutUTF8ToMAX_TAG_VALUE_BYTE_SIZE(MessageDetailVO.Tag_Value), StandardCharsets.UTF_8 );
                    theadDataAccess.stmt_INSERT_Message_Details.setString(3, ElementContentS );
                }
                else {
                    theadDataAccess.stmt_INSERT_Message_Details.setString(3, MessageDetailVO.Tag_Value);
                }
                theadDataAccess.stmt_INSERT_Message_Details.setInt(4, MessageDetailVO.Tag_Num);
                theadDataAccess.stmt_INSERT_Message_Details.setInt(5, MessageDetailVO.Tag_Par_Num);
                //theadDataAccess.stmt_INSERT_Message_Details.executeUpdate();
                theadDataAccess.stmt_INSERT_Message_Details.addBatch();
        /*MessegeSend_Log.info( i + ">" + theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "]" +
                "\n Tag_Id=" + MessageDetailVO.Tag_Id +
                "\n Tag_Value=" + MessageDetailVO.Tag_Value +
                "\n Tag_Num=" + MessageDetailVO.Tag_Num +
                "\n Tag_Par_Num=" + MessageDetailVO.Tag_Par_Num +
                " done");
                */
                nn = i;
            }
            // Insert data in Oracle with Java … Batched mode
            theadDataAccess.stmt_INSERT_Message_Details.executeBatch();

        } catch (SQLException e) {
            MessegeSend_Log.error(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "] :" + sStackTrace.strInterruptedException(e));
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeSend_Log.error("[{}] Hermes_Connection.rollback()fault: {}", Queue_Id, exp.getMessage());
            }
            return -2;
        }
        /* -- Commit делает вызов doUPDATE_MessageQueue_Out2Send() после успешной записи
        try {
            theadDataAccess.Hermes_Connection.commit();
        } catch (SQLException exp) {
            MessegeSend_Log.error("Hermes_Connection.commit() fault: " + exp.getMessage());
            return -3;
        }
        */
        MessegeSend_Log.info("Queue_Id=[{}] : Delete and INSERT new Message Details, {} rows done", Queue_Id, nn);
        return nn;
    }


    public static int SplitConfirmation(MessageDetails messageDetails, Element EntryElement, int tag_Par_Num,
                                        Logger MessegeSend_Log) {
        // Tag_Par_Num- №№ Тага, к которому прилепляем всё от EntryElement,ссылка на Tag_Num родителя
        // Tag_Num - сквозной!!! нумератор записей

        int nn = 0;
        //MessegeSend_Log.info("SplitConfirmation[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + EntryElement.getName() + ">");

        if ( EntryElement.getName().equals( XMLchars.TagConfirmation )) {
            messageDetails.ConfirmationRowNum = 0;
            String  ElementEntry = EntryElement.getName();
            //MessegeSend_Log.info("Tag_Par_Num[0]["+ messageDetails.Message_Tag_Num +"]: <" + ElementEntry + ">");
            MessageDetailVO messageDetailVO = new MessageDetailVO();
            messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                    "", // Tag_Value
                    messageDetails.Message_Tag_Num,
                    0
            );
            messageDetails.Confirmation.put(messageDetails.ConfirmationRowNum, messageDetailVO);
            messageDetails.ConfirmationRowNum += 1;
            // после заполнения данных для корневого элемента, для всех его детей нужен  Tag_Par_Num== messageDetails.Message_Tag_Num,
            // который был установлен ПЕРЕД кукурсивным SplitConfirmation!
            tag_Par_Num = messageDetails.Message_Tag_Num;

        }

        List<Element> Elements = EntryElement.getChildren();
        // Перебор всех элементов TemplConfig
        for (int i = 0; i < Elements.size(); i++) {
            Element XMLelement = (Element) Elements.get(i);

            String ElementEntry = XMLelement.getName();
            if (( XMLelement.getParentElement().getName().equals( XMLchars.TagConfirmation ) )
                    &&
                    (XMLelement.getName().equals(XMLchars.TagNext)) )
            {
                ; // пропускаем /Confirmation/Next
            }
            else {
                String ElementContent = XMLelement.getText();
                MessageDetailVO messageDetailVO = new MessageDetailVO();

                messageDetails.Message_Tag_Num += 1;

                if ( ElementContent.length() > 0 ) {
                    //MessegeSend_Log.info("Tag_Par_Num[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + ElementEntry + ">=" + ElementContent);
                    messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                            ElementContent, // Tag_Value
                            messageDetails.Message_Tag_Num,
                            tag_Par_Num
                    );
                } else {
                    //MessegeSend_Log.info("Tag_Par_Num[" + tag_Par_Num + "][" + messageDetails.Message_Tag_Num + "]: <" + ElementEntry + ">");

                    messageDetailVO.setMessageQueue(ElementEntry, // "Tag_Id"
                            "", // Tag_Value
                            messageDetails.Message_Tag_Num,
                            tag_Par_Num // Tag_Num += 1; будет сделано в Tag_Par_Num
                    );
                }
                messageDetails.Confirmation.put(messageDetails.ConfirmationRowNum, messageDetailVO);
                messageDetails.ConfirmationRowNum += 1;

                List<Attribute> ElementAttributes = XMLelement.getAttributes();
                for (int j = 0; j < ElementAttributes.size(); j++) {
                    Attribute XMLattribute = ElementAttributes.get(j);
                    MessageDetailVO ATTRmessageDetailVO = new MessageDetailVO();
                    String AttributeEntry = XMLattribute.getName();
                    String AttributeValue = XMLattribute.getValue();
                    // Attribute не увеличивает Tag_Num ( сквозной нумератор записей )
                    // в БД имеет Tag_Num= 0, ссылается на элемент.
                    //MessegeSend_Log.info("Tag_Par_Num[" + messageDetails.Message_Tag_Num + "][" + 0 + "]: \"" + AttributeEntry + "\"=" + AttributeValue);
                    ATTRmessageDetailVO.setMessageQueue(AttributeEntry, // "Tag_Id"
                            AttributeValue, // Tag_Value
                            0,
                            messageDetails.Message_Tag_Num
                    );
                    messageDetails.Confirmation.put(messageDetails.ConfirmationRowNum, ATTRmessageDetailVO);
                    messageDetails.ConfirmationRowNum += 1;
                }

                // int tag_Par_Num_4_Child = Tag_Num.intValue();
                SplitConfirmation(messageDetails, XMLelement,
                        messageDetails.Message_Tag_Num, // Tag_Par_Num  для рекурсии
                        // Tag_Num, // Tag_Num += 1; будет сделано в цикле по дочерним элементам перед добавлением
                        MessegeSend_Log);
            }

        }
        return nn;
    }


    public static int   ReplaceConfirmation(@NotNull TheadDataAccess theadDataAccess, long Queue_Id, @NotNull MessageDetails messageDetails, Logger MessegeSend_Log) {
        int nn = 0;

        nn = theadDataAccess.doDELETE_Message_Confirmation( Queue_Id, MessegeSend_Log);
        if ( nn < 0 )
            return -1;
        int iNumberRecordInConfirmation=0;

        try { MessegeSend_Log.warn("ReplaceConfirmation: {}", theadDataAccess.INSERT_Message_Details);
            for ( iNumberRecordInConfirmation = 0; iNumberRecordInConfirmation < messageDetails.Confirmation.size(); iNumberRecordInConfirmation++) {
                MessageDetailVO MessageDetailVO = messageDetails.Confirmation.get( iNumberRecordInConfirmation );
                theadDataAccess.stmt_INSERT_Message_Details.setLong(1, Queue_Id);
                theadDataAccess.stmt_INSERT_Message_Details.setString(2, MessageDetailVO.Tag_Id);
                // StringEscapeUtils.unescapeXml(MessageDetailVO.Tag_Value);
                //theadDataAccess.stmt_INSERT_Message_Details.setString(3, MessageDetailVO.Tag_Value);
                if ( MessageDetailVO.Tag_Value.length() > (XMLchars.MAX_TAG_VALUE_BYTE_SIZE /2) ) {
                    String ElementContentS = new String( XMLchars.cutUTF8ToMAX_TAG_VALUE_BYTE_SIZE(StringEscapeUtils.unescapeHtml4(MessageDetailVO.Tag_Value)), StandardCharsets.UTF_8 );
                    theadDataAccess.stmt_INSERT_Message_Details.setString(3, ElementContentS );
                }
                else {
                    theadDataAccess.stmt_INSERT_Message_Details.setString(3, StringEscapeUtils.unescapeHtml4(MessageDetailVO.Tag_Value) );
                }

                //MessegeSend_Log.warn("Queue_Id=[{}][{}]: {}={}", Queue_Id, MessageDetailVO.Tag_Num, MessageDetailVO.Tag_Id, StringEscapeUtils.unescapeHtml4(MessageDetailVO.Tag_Value));
                theadDataAccess.stmt_INSERT_Message_Details.setInt(4, MessageDetailVO.Tag_Num);
                theadDataAccess.stmt_INSERT_Message_Details.setInt(5, MessageDetailVO.Tag_Par_Num);

                // theadDataAccess.stmt_INSERT_Message_Details.executeUpdate();
                theadDataAccess.stmt_INSERT_Message_Details.addBatch();

                nn = iNumberRecordInConfirmation;
            }
            // Insert data in Oracle with Java … Batched mode
            theadDataAccess.stmt_INSERT_Message_Details.executeBatch();
        } catch ( Exception e) {
            MessegeSend_Log.error(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "]["+ iNumberRecordInConfirmation +"] :" + sStackTrace.strInterruptedException(e));
            messageDetails.MsgReason.append("ReplaceConfirmation [").append(iNumberRecordInConfirmation).append("] ").append(sStackTrace.strInterruptedException(e));
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeSend_Log.error("[{}] Hermes_Connection.rollback()fault: {}" , Queue_Id, exp.getMessage());
            }
            return -2;
        }
        try {
            theadDataAccess.Hermes_Connection.commit();
        } catch (SQLException exp) {
            MessegeSend_Log.error("[{}] Hermes_Connection.rollback()fault: {}" , Queue_Id, exp.getMessage());
            return -3;
        }

        MessegeSend_Log.info("{}:Queue_Id=[{}] :{} done", theadDataAccess.INSERT_Message_Details, Queue_Id, nn);
        return nn;
    }

}
