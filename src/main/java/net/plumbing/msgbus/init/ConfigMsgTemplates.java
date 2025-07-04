package net.plumbing.msgbus.init;
import net.sf.saxon.s9api.Processor;
import org.slf4j.Logger;
import net.plumbing.msgbus.model.MessageTemplateVO;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

/*import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.xml.sax.SAXException;
*/

public class ConfigMsgTemplates {

    public static int performConfig( MessageTemplateVO messageTemplateVO, Logger AppThead_log) {
        int sucess = 0;
        if ( messageTemplateVO == null ) return -1;
        String parsedConfig = messageTemplateVO.getConf_Text();
        //AppThead_log.info( parsedConfig );
        if ( parsedConfig == null ) return -2;
        if (parsedConfig.isEmpty()) return -3;

        try {
            SAXBuilder documentBuilder = new SAXBuilder();
            //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputStream parsedConfigStream = new ByteArrayInputStream( parsedConfig.getBytes(StandardCharsets.UTF_8));
            Document document = (Document)documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);

            Element  TemplConfig = document.getRootElement();
            List<Element> list = TemplConfig.getChildren();
            // Перебор всех элементов TemplConfig
            for (int i = 0; i < list.size(); i++) {
                Element TemplateElmnt = (Element) list.get(i);

                String configEntry = TemplateElmnt.getName();
                String configContent = TemplateElmnt.getText();
                if (configContent.isEmpty()) configContent = null;

                // AppThead_log.info( "configEntry:" + configEntry ); // + "\n configContent:" + configContent);
                switch ( configEntry) {
                    case "ConfigExecute":
                        messageTemplateVO.setConfigExecute( configContent );
                        if ( configContent!= null ) {
                            Properties properties = new Properties();
                            InputStream propertiesStream = new ByteArrayInputStream(configContent.getBytes(StandardCharsets.UTF_8));
                            properties.load(propertiesStream);
                            //проходимся по всем ключам и печатаем все их значения на консоль
                            for (String key : properties.stringPropertyNames()) {
                                // AppThead_log.info( "ConfigExecute: (" + properties.get(key) + ") " + key + " = " + properties.getProperty( key ) );
                                //System.out.println(properties.get(key));
                            }
                        }
                        break;
                    case "MessageXSD":
                        messageTemplateVO.setMessageXSD( configContent);
                        break;
                    case "HeaderXSLT":
                        messageTemplateVO.setHeaderXSLT( configContent);
                        if ( configContent!= null )
                            messageTemplateVO.makeHeaderXSLT_Transformer( AppThead_log );
                        break;

                    case  "ConfigPostExec":
                        messageTemplateVO.setConfigPostExec( configContent);
                        break;
                    case "EnvelopeXSLTPost":
                        messageTemplateVO.setEnvelopeXSLTPost( configContent);
                        if ( configContent!= null )
                            messageTemplateVO.makeEnvelopeXSLTPost_xslt30Transformer(AppThead_log);
                        break;

                    case "MsgAnswXSLT":
                        messageTemplateVO.setMsgAnswXSLT( configContent);
                        if ( configContent!= null )
                            messageTemplateVO.makeMsgAnswXSLT_xslt30Transformer(AppThead_log);
                        break;

                    case "MessageXSLT":
                        messageTemplateVO.setMessageXSLT( configContent);
                        if ( configContent!= null )
                            messageTemplateVO.makeMessageXSLT_xslt30Transformer(AppThead_log);
                        break;

                    case "AckXSLT":
                        messageTemplateVO.setAckXSLT( configContent);
                        if ( configContent!= null )
                            messageTemplateVO.makeAckXSLT_xslt30Transformer(AppThead_log);
                        break;

                    case "EnvelopeXSLTExt":
                        messageTemplateVO.setEnvelopeXSLTExt( configContent);
                        if ( configContent!= null )
                            messageTemplateVO.makeEnvelopeXSLTExt_xslt30Transformer(AppThead_log);
                        break;

                    case "ErrTransXSLT":
                        messageTemplateVO.setErrTransXSLT( configContent);
                        if ( configContent!= null )
                            messageTemplateVO.makeErrTransXSLT_xslt30Transformer(AppThead_log);
                        break;
                    case "EnvelopeNS" :
                        messageTemplateVO.setEnvelopeNS( configContent );
                }
            }
        } catch ( JDOMException |  IOException ex) {
            ex.printStackTrace(System.out);
        }
    // /TemplConfig/ConfigExecute


        return sucess;
    }
/***
    private static String getConfigExecute(Document document) throws DOMException, XPathExpressionException {
        //System.out.println("Example 1 - Печать всех элементов Cost");
        XPathFactory pathFactory = XPathFactory.newInstance();
        XPath xpath = pathFactory.newXPath();

        // Пример записи XPath
        // Подный путь до элемента

        XPathExpression expr = xpath.compile("//TemplConfig/ConfigExecute");

        NodeList nodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            return (n.getTextContent());
        }
        return null;
    }
**/
}
