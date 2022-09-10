package ru.hermes.msgbus.threads.utils;

import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.JDOMParseException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import ru.hermes.msgbus.common.XMLchars;
import ru.hermes.msgbus.common.sStackTracе;
import ru.hermes.msgbus.model.MessageDetailVO;
import ru.hermes.msgbus.model.MessageDetails;
import ru.hermes.msgbus.model.MessageQueueVO;
import ru.hermes.msgbus.model.MessageTemplate4Perform;
import ru.hermes.msgbus.threads.TheadDataAccess;
import ru.hermes.msgbus.ws.client.core.SoapClient;

import javax.validation.constraints.NotNull;
import javax.xml.xpath.XPathExpressionException;
import com.google.common.xml.XmlEscapers;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;


public class MessageSoapSend {

	// The SOAP server URI
	private String uriSOAPServer;
	// The SOAP connection
	// private static SOAPConnection soapConnection = null;

	// If you want to add namespace to the header, follow this constant
	private static final String PREFIX_NAMESPACE = "ns";
	private static final String NAMESPACE = "http://other.namespace.to.add.to.header";

/*
	public static int sendSOAPMessage(@NotNull MessageQueueVO messageQueueVO, @NotNull MessageDetails messageDetails, TheadDataAccess theadDataAccess, Logger MessegeSend_Log) {
		//
		// String xmlRequestBody = messageDetails.XML_MsgSEND;
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

		String EndPointUrl = messageTemplate4Perform.getEndPointUrl();
		Integer ConnectTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Conn() * 1000;
		Integer ReadTimeoutInMillis = messageTemplate4Perform.getPropTimeout_Read() * 1000;
		String Response;
		SoapClient SOAPClient = SoapClient.builder()
				.endpointUri(EndPointUrl)
				.connectTimeoutInMillis(ConnectTimeoutInMillis)
				.readTimeoutInMillis(ReadTimeoutInMillis)
				.build();

		MessegeSend_Log.info("SoapClient.builder(" + EndPointUrl + ").connectTimeoutInMillis=" + ConnectTimeoutInMillis +
				";.readTimeoutInMillis=ReadTimeoutInMillis");

		try {
			MessegeSend_Log.info("client.post:\n" + SoapEnvelope.toString());
			messageDetails.Confirmation.clear();
			messageDetails.XML_MsgResponse.setLength(0);
			messageDetails.XML_MsgResponse.append( //  получаем ответ от сервиса в виде XML-STRING
					SOAPClient.post(SoapEnvelope.toString(), MessegeSend_Log)
			);
			messageQueueVO.setRetry_Count(messageQueueVO.getRetry_Count() + 1);
			MessegeSend_Log.info("client.post:Response=(" + messageDetails.XML_MsgResponse.toString() + ")");
			// client.wait(100);
		} catch (Exception e) {
			MessegeSend_Log.error("sendSOAPMessage.SoapClient.post fault: " + sStackTracе.strInterruptedException(e));
			messageDetails.MsgReason.append(" sendSOAPMessage.SoapClient.post fault: " + sStackTracе.strInterruptedException(e));
			int messageRetry_Count = messageQueueVO.getRetry_Count();
			if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() ) {
				messageRetry_Count += 1; // увеличили счетчик попыток
				messageQueueVO.setRetry_Count(messageRetry_Count);
				// переводим время следующей обработки на  ShortRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
				theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getShortRetryInterval(),
						"Next attempt after " + messageDetails.MessageTemplate4Perform.getShortRetryInterval() + " sec., sendSOAPMessage.SoapClient.post fault: " + sStackTracе.strInterruptedException(e), 1236,
						messageRetry_Count, MessegeSend_Log
				);
				return -1;
			}
			if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount() ) {
				messageRetry_Count += 1; // увеличили счетчик попыток
				messageQueueVO.setRetry_Count(messageRetry_Count);
				// переводим время следующей обработки на  LongRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
				theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getLongRetryInterval(),
						"Next attempt after " + messageDetails.MessageTemplate4Perform.getLongRetryInterval() + " sec., sendSOAPMessage.SoapClient.post fault: " + sStackTracе.strInterruptedException(e), 1237,
						messageRetry_Count, MessegeSend_Log
				);
				return -1;
			}
			theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
					"sendSOAPMessage.SoapClient.post fault: " + sStackTracе.strInterruptedException(e), 1232,
					messageQueueVO.getRetry_Count(), MessegeSend_Log);
			return -1;
		}
		try {
			// Получили ответ от сервиса, инициируем обработку getResponseBody()
			getResponseBody(messageDetails, MessegeSend_Log);
			MessegeSend_Log.info("client.post:ClearBodyResponse=(" + messageDetails.XML_ClearBodyResponse.toString() + ")");
			// client.wait(100);

		} catch (Exception e) {
			MessegeSend_Log.error("sendSOAPMessage.getResponseBody fault: " + sStackTracе.strInterruptedException(e));
			messageDetails.MsgReason.append(" sendSOAPMessage.getResponseBod fault: " + sStackTracе.strInterruptedException(e));
			int messageRetry_Count = messageQueueVO.getRetry_Count();
			if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() ) {
				messageRetry_Count += 1; // увеличили счетчик попыток
				messageQueueVO.setRetry_Count(messageRetry_Count);
				// переводим время следующей обработки на  ShortRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
				theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getShortRetryInterval(),
						messageDetails.MsgReason.toString(), 1238,
						messageRetry_Count, MessegeSend_Log
				);
				return -3;
			}
			if ( messageRetry_Count < messageDetails.MessageTemplate4Perform.getShortRetryCount() + messageDetails.MessageTemplate4Perform.getLongRetryCount() ) {
				messageRetry_Count += 1; // увеличили счетчик попыток
				messageQueueVO.setRetry_Count(messageRetry_Count);
				// переводим время следующей обработки на  LongRetryInterval вперёд , сохраняя тот же MessageQueue_Direction
				theadDataAccess.doUPDATE_MessageQueue_DirectionAsIS(messageQueueVO.getQueue_Id(), messageDetails.MessageTemplate4Perform.getLongRetryInterval(),
						messageDetails.MsgReason.toString(), 1239,
						messageRetry_Count, MessegeSend_Log
				);
				return -3;
			}

			theadDataAccess.doUPDATE_MessageQueue_Send2ErrorOUT(messageQueueVO,
					messageDetails.MsgReason.toString(), 1232,
					messageQueueVO.getRetry_Count(), MessegeSend_Log);
			return -3;
		}

		return 0;
	}
*/
	public static String getResponseBody(@NotNull MessageDetails messageDetails, Logger MessegeSend_Log) throws JDOMParseException,JDOMException, IOException, XPathExpressionException {
		SAXBuilder documentBuilder = new SAXBuilder();
		//DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		InputStream parsedConfigStream = new ByteArrayInputStream(messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8));
		Document document = null;
		try {
		document = (Document) documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);
		}
		catch ( JDOMParseException e)
		{
			MessegeSend_Log.error("documentBuilder.build (" + messageDetails.XML_MsgResponse.toString() + ")fault"  );
			throw new JDOMParseException("client.post:getResponseBody=(" + messageDetails.XML_MsgResponse.toString() + ")", e);
		}

		Element SoapEnvelope = document.getRootElement();
		boolean isSoapBodyFinded = false;
		if ( SoapEnvelope.getName().equals(XMLchars.Envelope) ) {
			// MessegeSend_Log.error("debug HE-5865: SoapEnvelope.getName()= (" + SoapEnvelope.getName() + ")"  );
			// String deftarget = Envelope.getAttributeValue("default", "all");
			List<Element> list = SoapEnvelope.getChildren();
			// Перебор всех элементов Envelope
			for (int i = 0; i < list.size(); i++) {
				Element SoapElmnt = (Element) list.get(i);
				// MessegeSend_Log.error("debug HE-5865: SoapElmnt.getName()= (" + SoapElmnt.getName() + ")"  );
				if ( SoapElmnt.getName().equals(XMLchars.Body) ) {
					//MessegeSend_Log.info("client.post:getResponseBody=(" + SoapElmnt.getName());
					isSoapBodyFinded = true;

					// надо подготовить очищенный от ns: содержимое Body.
					messageDetails.Confirmation.clear();
					messageDetails.XML_ClearBodyResponse.setLength(0);
					SoapBody2XML_String(messageDetails, SoapElmnt, MessegeSend_Log);
				}
			}

			if ( !isSoapBodyFinded ) {
				MessegeSend_Log.error("documentBuilder.build (" + messageDetails.XML_MsgResponse.toString().getBytes(StandardCharsets.UTF_8) + ")fault"  );
				throw new XPathExpressionException("getResponseBody: в SOAP-ответе не найден Element=" + XMLchars.Body);
			}
		} else {

			throw new XPathExpressionException("getResponseBody: в SOAP-ответе("
			+ (messageDetails.XML_MsgResponse.length() > 512 ? messageDetails.XML_MsgResponse.substring(0, 512) : messageDetails.XML_MsgResponse.toString())
			+ "...) не найден RootElement=" + XMLchars.Envelope);
		}

		return null;
	}


	public static int SoapBody2XML_String(@NotNull MessageDetails messageDetails, Element SoapBody, Logger MessegeSend_Log) {
		MessageDetailVO messageDetailVO = messageDetails.Message.get(0);
		int BodyListSize = 0;
		if ( messageDetailVO == null) return BodyListSize; // debug HE-5865:
		// LinkedList<MessageDetailVO> linkedTags = new LinkedList<>();
		// linkedTags.clear();
		if ( messageDetailVO.Tag_Num != 0 ) {
			List<Element> list = SoapBody.getChildren();
			// Перебор всех элементов Envelope
			for (int i = 0; i < list.size(); i++) {
				Element SoapElmnt = (Element) list.get(i);
				//MessegeSend_Log.info("client.post:SoapBody2XML_String=(\n" + SoapElmnt.getName() + " =" + SoapElmnt.getText() + "\n");
				// надо подготовить очищенный от ns: содержимое Body.
				messageDetails.XML_ClearBodyResponse.append(XMLchars.OpenTag + SoapElmnt.getName() + XMLchars.CloseTag);
				MessageSoapSend.XML_BodyElemets2StringB(messageDetails, SoapElmnt, MessegeSend_Log);
				messageDetails.XML_ClearBodyResponse.append(XMLchars.OpenTag + XMLchars.EndTag + SoapElmnt.getName() + XMLchars.CloseTag);
				// MessegeSend_Log.info("SoapBody2XML_String(XML_ClearBodyResponse):" + messageDetails.XML_ClearBodyResponse.toString());
			}
		}
		return BodyListSize;

	}

	public static int XML_BodyElemets2StringB(MessageDetails messageDetails, Element EntryElement,
	                                           Logger MessegeSend_Log) {

		int nn = 0;
		// MessegeSend_Log.info("XML_BodyElemets2StringB: <" + EntryElement.getName() + ">");
		List<Element> Elements = EntryElement.getChildren();
		// Перебор всех элементов TemplConfig
		for (int i = 0; i < Elements.size(); i++) {
			Element XMLelement = (Element) Elements.get(i);
			String ElementEntry = XMLelement.getName();
			String ElementContent = XmlEscapers.xmlAttributeEscaper().escape( XMLelement.getText()); //.getText(); заменил на getValue() из-за "<>"

			messageDetails.XML_ClearBodyResponse.append(XMLchars.OpenTag + ElementEntry);
			//MessegeSend_Log.info("XML_BodyElemets2StringB {<" + ElementEntry + ">}");

			List<Attribute> ElementAttributes = XMLelement.getAttributes();
			for (int j = 0; j < ElementAttributes.size(); j++) {
				Attribute XMLattribute = ElementAttributes.get(j);

				String AttributeEntry = XMLattribute.getName();
				String AttributeValue = XmlEscapers.xmlAttributeEscaper().escape( XMLattribute.getValue());

				messageDetails.XML_ClearBodyResponse.append(XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote);
				//MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote + "}");
			}
			messageDetails.XML_ClearBodyResponse.append(XMLchars.CloseTag);

			if ( ElementContent.length() > 0 ) {
				messageDetails.XML_ClearBodyResponse.append(ElementContent);
				// MessegeSend_Log.info("XML_BodyElemets2StringB[" + ElementContent + "]");
			}


			XML_BodyElemets2StringB(messageDetails, XMLelement,
					MessegeSend_Log);
			messageDetails.XML_ClearBodyResponse.append(XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag);
			//MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag + "}");

		}
		return nn;
	}

}
