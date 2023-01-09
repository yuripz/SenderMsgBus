package net.plumbing.msgbus.threads.utils;

import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.json.XML;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.threads.TheadDataAccess;
import org.apache.commons.io.IOUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.JDOMParseException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.jdom2.filter.Filters;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import static net.plumbing.msgbus.common.XMLchars.OpenTag;

public class ShellScripExecutor {
    public static int execShell (MessageQueueVO messageQueueVO, MessageDetails messageDetails, TheadDataAccess theadDataAccess, Logger MessegeSend_Log )  {
        ;
        ;
        String cmdLine = messageDetails.MessageTemplate4Perform.getPropShellScriptExeFullPathName();
        String XPathParams = messageDetails.MessageTemplate4Perform.getPropXPathParams();
        /* .ConfigExecute.prop:
        cmdLine =>
        ShellScript=sh -c /home/oracle/HE-3997_Hermes_APD_Integration/runDocument2DWH.sh OpderId=
        XPathParams =>
        XPathParams=/callScript4Document2DWH/payload/orderId
         */

        if (cmdLine == null ) {
            String errorMessage = "В шаблоне не куказано свойство `" + messageDetails.MessageTemplate4Perform.ShellScriptMethod + "`, в котором ожидается получение командной строки на запуск";

            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    errorMessage, true,
                    null, MessegeSend_Log);
            MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] " + errorMessage);
            return -601;
        }
        String Param_Value_4_cmd = "";
        if ( XPathParams != null ) {
            // Готовим XML-документ из Message.XML_MsgSEND для получения дополгительных параметров
            Document Input_Clear_XMLDocument = null;
            SAXBuilder documentBuilder = new SAXBuilder();
            try (InputStream parsedXML_MsgClearStream = new ByteArrayInputStream(messageDetails.XML_MsgSEND.getBytes(StandardCharsets.UTF_8))) {
                Input_Clear_XMLDocument = documentBuilder.build(parsedXML_MsgClearStream); // .parse(parsedConfigStream);
            } catch (IOException | JDOMException e) {
                MessegeSend_Log.error("documentBuilder.build (" + messageDetails.XML_MsgSEND + ")fault");

            }
            XPathExpression<Element> xpathTemplate_Id = XPathFactory.instance().compile(XPathParams, Filters.element());
            Element elmtTemplate_Id = xpathTemplate_Id.evaluateFirst(Input_Clear_XMLDocument);
            Param_Value_4_cmd = elmtTemplate_Id.getText();
        }

        String[] cmdParams;
        String runCmdLine = cmdLine + Param_Value_4_cmd;


        cmdParams =runCmdLine.split(" ");
        MessegeSend_Log.info("exec:" + runCmdLine );
        ProcessBuilder processBuilder = new ProcessBuilder( cmdParams );
        //List<String> builderList = new  ArrayList<>();
        processBuilder.command(cmdParams);
//        processBuilder.redirectOutput();
        // processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process process = null;
        try {

           process =  processBuilder.start();
        }
        catch ( IOException e) {
            e.printStackTrace();
            String errorMessage = "processBuilder.start fault:" + e.getMessage();
            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    errorMessage, true,
                    null, MessegeSend_Log);
            MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] " + errorMessage);
            return -602;
        }
        BufferedReader iReader = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
        String tempStr= "";
        StringBuilder iBuffer = new StringBuilder();
        try {
            while((tempStr = iReader.readLine())!=null)  {
                iBuffer.append(tempStr+System.lineSeparator());
            }
        }
        catch ( IOException e) {
            e.printStackTrace();
            String errorMessage = "process.getInputStream.readLine fault:" + e.getMessage();
            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    errorMessage, true,
                    null, MessegeSend_Log);
            MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] " + errorMessage);
            process.destroy();
            return -603;
        }
        System.out.println("outputReader:" + iBuffer.toString());
/*
        BufferedReader eReader = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
        StringBuilder eBuffer = new StringBuilder();
        while((tempStr = eReader.readLine())!=null){
            eBuffer.append(tempStr+System.lineSeparator());
        }
        */
        String eBuffer =null;
        try {
        BufferedReader xReader = process.errorReader( StandardCharsets.UTF_8 ) ; // Charset.forName("cp866"));
         eBuffer =  IOUtils.toString( xReader);
        }
        catch ( IOException e) {
            e.printStackTrace();
            String errorMessage = "process.errorReader.IOUtils.toString fault:" + e.getMessage();
            MessageUtils.ProcessingSendError(messageQueueVO, messageDetails, theadDataAccess,
                    errorMessage, true,
                    null, MessegeSend_Log);
            MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] " + errorMessage);
            process.destroy();
            return -604;
        }

        MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] errorReader:" + eBuffer);
        String ElementContentS = new String( XMLchars.cutUTF8ToMAX_TAG_VALUE_BYTE_SIZE(eBuffer), StandardCharsets.UTF_8 );

        process.destroy();
        // checkExec
        Integer exitValue;
        try {
            exitValue= process.exitValue();
        }
        catch ( IllegalThreadStateException e) {
            exitValue = -255;
        }

        MessegeSend_Log.info ( "[" + messageQueueVO.getQueue_Id() + "] Exit value =[" + exitValue + "] for `" + runCmdLine + "`" );
        messageDetails.XML_MsgResponse.append(XMLchars.Envelope_Begin);
        messageDetails.XML_MsgResponse.append(XMLchars.Body_Begin);
        messageDetails.XML_MsgResponse.append( XMLchars.nanXSLT_Result );
        messageDetails.XML_MsgResponse.append(XMLchars.Body_End);
        messageDetails.XML_MsgResponse.append(XMLchars.Envelope_End);

    // надо подготовить очищенный от ns: содержимое Body.
        messageDetails.Confirmation.clear();
        messageDetails.XML_ClearBodyResponse.setLength(0);
        messageDetails.XML_ClearBodyResponse.append(OpenTag).append( "ResponseCallScript" ).append( XMLchars.CloseTag);
        messageDetails.XML_ClearBodyResponse.append(OpenTag).append( "Exit_Value" ).append( XMLchars.CloseTag);
        messageDetails.XML_ClearBodyResponse.append( exitValue.toString() );
        messageDetails.XML_ClearBodyResponse.append(OpenTag).append( XMLchars.EndTag ).append( "Exit_Value" ).append( XMLchars.CloseTag);
        messageDetails.XML_ClearBodyResponse.append(OpenTag).append( "responseBody" ).append( XMLchars.CloseTag);
        messageDetails.XML_ClearBodyResponse.append(ElementContentS);
        messageDetails.XML_ClearBodyResponse.append(OpenTag).append( XMLchars.EndTag ).append( "responseBody" ).append( XMLchars.CloseTag);
        messageDetails.XML_ClearBodyResponse.append(OpenTag).append( XMLchars.EndTag ).append( "ResponseCallScript" ).append( XMLchars.CloseTag);
        if ( messageDetails.MessageTemplate4Perform.getIsDebugged() )
            MessegeSend_Log.info("Unirest.post:ClearBodyResponse=(" + messageDetails.XML_ClearBodyResponse.toString() + ")");


        return 0;
    }
}
