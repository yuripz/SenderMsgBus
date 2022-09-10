package ru.hermes.msgbus.threads.utils;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jdom2.*;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.slf4j.Logger;
import ru.hermes.msgbus.common.XMLchars;
import ru.hermes.msgbus.common.sStackTracе;
import ru.hermes.msgbus.model.MessageDetailVO;
import ru.hermes.msgbus.model.MessageDetails;
import ru.hermes.msgbus.model.MessageDirections;
import ru.hermes.msgbus.model.MessageQueueVO;
import ru.hermes.msgbus.threads.TheadDataAccess;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.regex.Pattern;

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

    public static Integer ProcessingSendError(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess,
                                              String whyIsFault , boolean isMessageQueue_Directio_2_ErrorOUT, Exception e , Logger MessegeSend_Log)
    {
        String ErrorExceptionMessage;
        if ( e != null ) {
            ErrorExceptionMessage = sStackTracе.strInterruptedException(e);
        }
        else ErrorExceptionMessage = ";";


        int messageRetry_Count = messageQueueVO.getRetry_Count();
        messageRetry_Count += 1; // увеличили счетчик попыток
        if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() ) {

            messageQueueVO.setRetry_Count(messageRetry_Count);
            // переводим время следующей обработки на  ShortRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
            theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getShortRetryInterval(),
                    "Next attempt after " + messageDetails.MessageTemplate4Perform.getShortRetryInterval() + " sec.," + whyIsFault + "fault: " + ErrorExceptionMessage, 1236,
                    messageRetry_Count, MessegeSend_Log
            );
            messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now(ZoneId.of( "Europe/Moscow") ) )   );
            messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
            return -1;
        }
        if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount() ) {

            messageQueueVO.setRetry_Count(messageRetry_Count);
            // переводим время следующей обработки на  LongRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
            theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getLongRetryInterval(),
                    "Next attempt after " + messageDetails.MessageTemplate4Perform.getLongRetryInterval() + " sec.," + whyIsFault + "fault: " + ErrorExceptionMessage, 1237,
                    messageRetry_Count, MessegeSend_Log
            );
            messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now(ZoneId.of( "Europe/Moscow") ) )   );
            messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
            return -1;
        }
        if ( isMessageQueue_Directio_2_ErrorOUT ) // Если это не Транспортная ошибка, то выставляем ERROROUT
        {
            theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                    whyIsFault + " fault: " + ErrorExceptionMessage, 1239,
                    messageQueueVO.getRetry_Count(), MessegeSend_Log);
            messageQueueVO.setQueue_Direction(XMLchars.DirectERROUT);
        }
        return -1;
    }

    public static Integer ProcessingOut2ErrorOUT(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess,
                                             String whyIsFault , Exception e , Logger MessegeSend_Log)
    {
        String ErrorExceptionMessage;
        if ( e != null ) {
            ErrorExceptionMessage = sStackTracе.strInterruptedException(e);
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
            ErrorExceptionMessage = sStackTracе.strInterruptedException(e);
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
        int nn = 0;
        StringBuilder SoapEnvelope = new StringBuilder(XMLchars.Envelope_noNS_Begin);
        SoapEnvelope.append(XMLchars.Header_noNS_Begin);
        SoapEnvelope.append("<MsgId>" + messageQueueVO.getQueue_Id() +"</MsgId>");
        SoapEnvelope.append(XMLchars.Header_noNS_End);

        SoapEnvelope.append(XMLchars.Body_noNS_Begin);
        SoapEnvelope.append( messageDetails.XML_MsgRESOUT );
        SoapEnvelope.append(XMLchars.Body_noNS_End);
        SoapEnvelope.append(XMLchars.Envelope_noNS_End);

        return SoapEnvelope.toString();
    }

    public static String PrepareEnvelope4ErrTransXSLT( MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, Logger MessegeSend_Log) {
        int nn = 0;
        StringBuilder SoapEnvelope = new StringBuilder(XMLchars.Envelope_noNS_Begin);
        SoapEnvelope.append(XMLchars.Header_noNS_Begin);
        SoapEnvelope.append("<MsgId>" + messageQueueVO.getQueue_Id() +"</MsgId>");
        SoapEnvelope.append(XMLchars.Header_noNS_End);

        SoapEnvelope.append(XMLchars.Body_noNS_Begin);
        SoapEnvelope.append( XMLchars.DirectERROUT );
        SoapEnvelope.append(XMLchars.Body_noNS_End);
        SoapEnvelope.append(XMLchars.Envelope_noNS_End);

        return SoapEnvelope.toString();
    }

    public static String PrepareConfirmation(TheadDataAccess theadDataAccess, MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, Logger MessegeSend_Log) {
        int nn = 0;
        messageDetails.Confirmation.clear();

        String parsedMessageConfirmation = messageDetails.XML_MsgRESOUT.toString();
        String AnswXSLTQueue_Direction = XMLchars.DirectERROUT;
        //AppThead_log.info( parsedConfig );
        if ( parsedMessageConfirmation == null ) {
            theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                    "PrepareConfirmation: результат преобоазования MsgAnswXSLT ( или чистый Response) - пустая строка" + messageDetails.MessageTemplate4Perform.getMsgAnswXSLT(), 1232,
                    messageQueueVO.getRetry_Count(), MessegeSend_Log);
            return AnswXSLTQueue_Direction;
        }
        if ( parsedMessageConfirmation.length() == 0 ) {
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
                        MessegeSend_Log.info(" ["+ messageQueueVO.getQueue_Id() +"] XPath has result: <" + emtNext.getName() + "> :" + emtNext.getText()
                        );
                        AnswXSLTQueue_Direction = emtNext.getText();

                        XPathExpression<Element> xpathResultCode = XPathFactory.instance().compile(xpathResultCodeExpression, Filters.element());
                        Element emtResultCode = xpathResultCode.evaluateFirst(document);
                        if ( emtResultCode != null )
                            try {
                                iMsgStaus = Integer.parseInt(emtResultCode.getText());
                            } catch (NumberFormatException e) {
                                iMsgStaus = 12345;
                                messageDetails.MsgReason.append("Не не получили числового значения Статуса " + xpathResultCodeExpression + " в результате XSLT преобразования Response");
                            }
                        else {
                            messageDetails.MsgReason.append("Не нашли " + xpathResultCodeExpression + " в результате XSLT преобразования Response");
                            iMsgStaus = 12346;
                        }

                        XPathExpression<Element> xpathMessage = XPathFactory.instance().compile(xpathMessageExpression, Filters.element());
                        Element emtMessage = xpathMessage.evaluateFirst(document);
                        if ( emtMessage != null )
                            messageDetails.MsgReason.append(emtMessage.getText());
                        else
                            messageDetails.MsgReason.append("Не нашли " + xpathMessageExpression + " в " + parsedMessageConfirmation);

                        if ( emtConfirmation != null ) {

                            // получаем МАХ Tag_Num из messageDetails.Message
                            messageDetails.Message_Tag_Num = 0;
                            for (int i = 0; i < messageDetails.Message.size(); i++) {
                                if ( messageDetails.Message_Tag_Num < messageDetails.Message.get(i).Tag_Num )
                                    messageDetails.Message_Tag_Num = messageDetails.Message.get(i).Tag_Num;
                            }
                            messageDetails.Message_Tag_Num += 1; // Установили Tag_Num для SplitConfirmation
                            messageDetails.Confirmation.clear();

                            // Split, которая из него сделает набор записей messageDetails.Message -> HashMap<Integer, MessageDetailVO>
                            SplitConfirmation(messageDetails, emtConfirmation, 0, // Tag_Num = messageDetails.Message_Tag_Num !
                                    MessegeSend_Log);

                            // Замещаем полученным массивом messageDetails.Message строки в БД

                            nn = MessageUtils.ReplaceConfirmation(theadDataAccess, messageQueueVO.getQueue_Id(), messageDetails, MessegeSend_Log);
                            if ( nn < 0 ) { //
                                theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                                        "PrepareConfirmation.'" + StringEscapeUtils.unescapeHtml4(messageDetails.MsgReason.toString()) + "' for (" + parsedMessageConfirmation + ")", 1233,
                                        messageQueueVO.getRetry_Count(), MessegeSend_Log);
                                MessegeSend_Log.error("PrepareConfirmation.'"+ messageDetails.MsgReason.toString()  +"' for(" + parsedMessageConfirmation + ")");

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
                        MessegeSend_Log.error("PrepareConfirmation.XPathFactory.xpath.evaluateFirst('\"+ xpathNextExpression  +\"') не нашёл <Next></Next> в (" + parsedMessageConfirmation + ")");

                        return AnswXSLTQueue_Direction;
                    }
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                    MessegeSend_Log.error("PrepareConfirmation.XPathFactory.xpath.evaluateFirst \""+ xpathNextExpression + "\" fault: " + ex.getMessage() + " for " + parsedMessageConfirmation );
                    theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                            "PrepareConfirmation.XPathFactory.xpath.evaluateFirst \""+ xpathNextExpression +"\" for (" + parsedMessageConfirmation + ") fault: " + ex.getMessage()  , 1233,
                            messageQueueVO.getRetry_Count(), MessegeSend_Log);
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                MessegeSend_Log.error("PrepareConfirmation.XPathFactory.xpath.evaluateFirst '/Confirmation' fault: " + ex.getMessage() + " for " + parsedMessageConfirmation );
                theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                        "PrepareConfirmation.XPathFactory.xpath.evaluateFirst \"/Confirmation\" for (" + parsedMessageConfirmation + ") fault: " + ex.getMessage()  , 1233,
                        messageQueueVO.getRetry_Count(), MessegeSend_Log);

                return AnswXSLTQueue_Direction;
            }
            // xml-документ в виде строки = messageDetails.XML_MsgSEND поступает
            // Split, которая из него сделает набор записей messageDetails.Message -> HashMap<Integer, MessageDetailVO>


        } catch (JDOMException | IOException ex) {
            MessegeSend_Log.error("PrepareConfirmation.documentBuilder fault: " + ex.getMessage() + " for " + parsedMessageConfirmation);
            theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
                    "PrepareConfirmation.documentBuilder fault: " + ex.getMessage() + " for " + parsedMessageConfirmation, 1233,
                    messageQueueVO.getRetry_Count(), MessegeSend_Log);
            return AnswXSLTQueue_Direction;
        }

        return AnswXSLTQueue_Direction;
    }
    public static int ReadMessage(TheadDataAccess theadDataAccess, long Queue_Id, @NotNull MessageDetails messageDetails, Logger MessegeSend_Log) {

        messageDetails.Message.clear();
        messageDetails.MessageRowNum = 0;
        messageDetails.Message_Tag_Num = 0;

        Pattern pattern = Pattern.compile("\\d+");
        try {
            theadDataAccess.stmtMsgQueueDet.setLong(1, Queue_Id);
            ResultSet rs = theadDataAccess.stmtMsgQueueDet.executeQuery();
            String rTag_Value=null;
            String rTag_Id=null;
            String xmlTag_Id;
            while (rs.next()) {
                MessageDetailVO messageDetailVO = new MessageDetailVO();
                rTag_Value = rs.getString("Tag_Value");
                rTag_Id= rs.getString("Tag_Id");
                if ( pattern.matcher( rTag_Id ).matches() )
                    xmlTag_Id = "Element" + rTag_Id;
                else
                    xmlTag_Id = rTag_Id;
                if ( rTag_Value == null )
                messageDetailVO.setMessageQueue(
                        xmlTag_Id,
                        null,
                        rs.getInt("Tag_Num"),
                        rs.getInt("Tag_Par_Num")
                );
                else
                    messageDetailVO.setMessageQueue(
                            xmlTag_Id,
                            StringEscapeUtils.escapeXml10(stripNonValidXMLCharacters(rTag_Value)),
                            //StringEscapeUtils.escapeXml10(rTag_Value.replaceAll(XMLchars.XML10pattern,"")),
                            // StringEscapeUtils.escapeXml10(rTag_Value).replaceAll(XMLchars.XML10pattern,""),
                            rs.getInt("Tag_Num"),
                            rs.getInt("Tag_Par_Num")
                    );
                messageDetails.Message.put(messageDetails.MessageRowNum, messageDetailVO);
                messageDetails.MessageRowNum += 1;
                // MessegeSend_Log.info( "Tag_Id:" + rs.getString("Tag_Id") + " [" + rs.getString("Tag_Value") + "]");
            }
        } catch (SQLException e) {
            MessegeSend_Log.error("Queue_Id=[" + Queue_Id + "] :" + sStackTracе.strInterruptedException(e));
            e.printStackTrace();
            return messageDetails.MessageRowNum;
        }
        if ( messageDetails.MessageRowNum > 0 )
        XML_Current_Tags(messageDetails, 0);
        MessegeSend_Log.info(messageDetails.XML_MsgOUT.toString());
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
    public static int XML_Current_Tags(@NotNull MessageDetails messageDetails, int Current_Elm_Key) {
        MessageDetailVO messageDetailVO = messageDetails.Message.get(Current_Elm_Key);

        if ( messageDetailVO.Tag_Num != 0 ) {

            // !было:  StringBuilder XML_Tag = new StringBuilder( XMLchars.OpenTag + messageDetailVO.Tag_Id );
            // стало:
            messageDetails.XML_MsgOUT.append(XMLchars.OpenTag + messageDetailVO.Tag_Id);
            // XML_Tag.append ( "<" + messageDetailVO.Tag_Id + ">" );
            // цикл по формированию параметров-аьтрибутов элемента
            for (int i = 0; i < messageDetails.Message.size(); i++) {
                MessageDetailVO messageChildVO = messageDetails.Message.get(i);
                if ( (messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент
                        (messageChildVO.Tag_Num == 0) )  // это атрибут элемента, у которого нет потомков
                {
                    if ( messageChildVO.Tag_Value != null )
                        // !было:XML_Tag.append ( XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + messageChildVO.Tag_Value + XMLchars.Quote );
                        // стало:
                        messageDetails.XML_MsgOUT.append(XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + messageChildVO.Tag_Value + XMLchars.Quote);
                    else
                        // !было: XML_Tag.append ( XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + XMLchars.Quote );
                        // стало:
                        messageDetails.XML_MsgOUT.append(XMLchars.Space + messageChildVO.Tag_Id + XMLchars.Equal + XMLchars.Quote + XMLchars.Quote);
                }
            }
            // !было: XML_Tag.append( XMLchars.CloseTag);
            // стало:
            messageDetails.XML_MsgOUT.append(XMLchars.CloseTag);

            if ( messageDetailVO.Tag_Value != null )
                // !было: XML_Tag.append( messageDetailVO.getTag_Value() );
                // стало:
                messageDetails.XML_MsgOUT.append(messageDetailVO.getTag_Value());

            for (int i = 0; i < messageDetails.Message.size(); i++) {
                MessageDetailVO messageChildVO = messageDetails.Message.get(i);
                if ( (messageChildVO.Tag_Par_Num == messageDetailVO.Tag_Num) && // нашли Дочерний элемент
                        (messageChildVO.Tag_Num != 0) )  // И это элемент, который может быть потомком!
                {
                    // !было: XML_Tag.append ( XML_Current_Tags( messageDetails, i) );
                    // стало:
                    XML_Current_Tags(messageDetails, i);
                }
            }

            // !было: XML_Tag.append( XMLchars.OpenTag + XMLchars.EndTag + messageDetailVO.Tag_Id + XMLchars.CloseTag);
            // стало:
            messageDetails.XML_MsgOUT.append(XMLchars.OpenTag + XMLchars.EndTag + messageDetailVO.Tag_Id + XMLchars.CloseTag);
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
            MessegeSend_Log.error("[" + Queue_Id + "] ReplaceMessage4SEND fault:" + ex.getMessage());
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
            MessegeSend_Log.error("DELETE(" + theadDataAccess.DELETE_Message_Details + "[" + Queue_Id + "]" + ") fault: " + e.getMessage());
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeSend_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
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
            MessegeSend_Log.error(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "] :" + sStackTracе.strInterruptedException(e));
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeSend_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
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
        MessegeSend_Log.info(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "] : Delete and INSERT new Message Details, " + nn + " rows done");
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


    public static int ReplaceConfirmation(@NotNull TheadDataAccess theadDataAccess, long Queue_Id, @NotNull MessageDetails messageDetails, Logger MessegeSend_Log) {
        int nn = 0;

        nn = theadDataAccess.doDELETE_Message_Confirmation( Queue_Id, MessegeSend_Log);
        if ( nn < 0 )
            return -1;
        int iNumberRecordInConfirmation=0;

        try {
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

                // MessegeSend_Log.error(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "]["+  MessageDetailVO.Tag_Id +"] :" + StringEscapeUtils.unescapeHtml4(MessageDetailVO.Tag_Value));
                theadDataAccess.stmt_INSERT_Message_Details.setInt(4, MessageDetailVO.Tag_Num);
                theadDataAccess.stmt_INSERT_Message_Details.setInt(5, MessageDetailVO.Tag_Par_Num);

                // theadDataAccess.stmt_INSERT_Message_Details.executeUpdate();
                theadDataAccess.stmt_INSERT_Message_Details.addBatch();

                nn = iNumberRecordInConfirmation;
            }
            // Insert data in Oracle with Java … Batched mode
            theadDataAccess.stmt_INSERT_Message_Details.executeBatch();
        } catch ( Exception e) {
            MessegeSend_Log.error(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "]["+ iNumberRecordInConfirmation +"] :" + sStackTracе.strInterruptedException(e));
            messageDetails.MsgReason.append( "ReplaceConfirmation ["+ iNumberRecordInConfirmation +"] " + sStackTracе.strInterruptedException(e) );
            e.printStackTrace();
            try {
                theadDataAccess.Hermes_Connection.rollback();
            } catch (SQLException exp) {
                MessegeSend_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
            }
            return -2;
        }
        try {
            theadDataAccess.Hermes_Connection.commit();
        } catch (SQLException exp) {
            MessegeSend_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
            return -3;
        }

        MessegeSend_Log.info(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "] :" + nn + " done");
        return nn;
    }

}
