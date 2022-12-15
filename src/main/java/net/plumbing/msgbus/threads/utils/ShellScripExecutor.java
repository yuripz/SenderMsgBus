package net.plumbing.msgbus.threads.utils;

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

public class ShellScripExecutor {
    public static int execShell (MessageQueueVO messageQueueVO, MessageDetails Message, TheadDataAccess theadDataAccess, Logger MessegeSend_Log )  {
        ;
        ;
        String cmdLine = Message.MessageTemplate4Perform.getPropShellScriptExeFullPathName();
        String XPathParams = Message.MessageTemplate4Perform.getPropXPathParams();

        if (cmdLine == null ) {
            String errorMessage = "В шаблоне не куказано свойство `" + Message.MessageTemplate4Perform.ShellScriptMethod + "`, в котором ожидается получение командной строки на запуск";

            MessageUtils.ProcessingSendError(messageQueueVO, Message, theadDataAccess,
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
            try (InputStream parsedXML_MsgClearStream = new ByteArrayInputStream(Message.XML_MsgSEND.getBytes(StandardCharsets.UTF_8))) {
                Input_Clear_XMLDocument = documentBuilder.build(parsedXML_MsgClearStream); // .parse(parsedConfigStream);
            } catch (IOException | JDOMException e) {
                MessegeSend_Log.error("documentBuilder.build (" + Message.XML_MsgSEND + ")fault");

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
            MessageUtils.ProcessingSendError(messageQueueVO, Message, theadDataAccess,
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
            MessageUtils.ProcessingSendError(messageQueueVO, Message, theadDataAccess,
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
        BufferedReader xReader = process.errorReader( Charset.forName("cp866"));
         eBuffer =  IOUtils.toString( xReader);
        }
        catch ( IOException e) {
            e.printStackTrace();
            String errorMessage = "process.errorReader.IOUtils.toString fault:" + e.getMessage();
            MessageUtils.ProcessingSendError(messageQueueVO, Message, theadDataAccess,
                    errorMessage, true,
                    null, MessegeSend_Log);
            MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] " + errorMessage);
            process.destroy();
            return -604;
        }

        MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + "] errorReader:" + eBuffer);

        process.destroy();
        // checkExec
        int exitValue = process.exitValue();
        MessegeSend_Log.info ( "[" + messageQueueVO.getQueue_Id() + "] Exit value =[" + exitValue + "] for `" + runCmdLine + "`" );


        return 0;
    }
}
