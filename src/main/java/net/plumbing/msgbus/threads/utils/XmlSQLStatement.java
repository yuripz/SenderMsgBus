package net.plumbing.msgbus.threads.utils;

import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.threads.TheadDataAccess;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.slf4j.Logger;
import net.plumbing.msgbus.common.sStackTrace;
import net.plumbing.msgbus.model.MessageQueueVO;

import javax.validation.constraints.NotNull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;

public class XmlSQLStatement {

    public static final String  TagNameHead       = "SQLRequest";

    public static final String  TagNameStatement  = "SQLStatement";
    public static final String  AttrNameStateType  = "type";
    public static final String  AttrNameStateNum   = "snum";
    public static final String  TagNamePSTMT      = "PSTMT";
    public static final String  TagNameParam      = "Param";
    public static final String  AttrNameParamNum  = "pnum";
    public static final String  AttrNameParamType = "type";
    public static final String  AttrNameParamDir  = "dir";

    public static final String  TagNameResultSet  = "ResultSet";
    public static final String  TagNameResult     = "Result";
    public static final String  AttrNameResNum    = "rnum";
    public static final String  TagNameReturn     = "Return";
    public static final String  TagNameRetNorm    = "Normal";
    public static final String  TagNameRetFault   = "Fault";

    public static final String  ParTypeString   = "string";
    public static final String  ParTypeNumber   = "number";
    public static final String  ParTypeDirIN    = "in";
    public static final String  ParTypeDirOUT   = "out";

    public static final String  OperTypeSel     = "select";
    public static final String  OperTypeIns     = "insert";
    public static final String  OperTypeUpdt    = "update";
    public static final String  OperTypeDel     = "delete";
    public static final String  OperTypeFunc    = "function";

    public static int ExecuteSQLincludedXML(TheadDataAccess theadDataAccess,
                                            String Passed_Envelope4XSLTPost,
                                            MessageQueueVO messageQueueVO,
                                            @NotNull MessageDetails messageDetails, boolean isDebugged, Logger MessegeSend_Log) {
        int nn = 0;
       // TODO ExecuteSQLincludedXML не должен Message.clear() и сбрасывать MessageRowNum = 0, но это надо тестить
        // messageDetails.Message.clear();
       // messageDetails.MessageRowNum = 0;
       // messageDetails.Message_Tag_Num = 0;
        messageDetails.MsgReason.setLength(0);
       // messageDetails.MsgReason.append("ExecuteSQLinXML is not ready yet! ");

        CallableStatement callableStatement;
        final String SQLcallableStatementExpression;
        final String SQLparamValue;


        try {
            SAXBuilder documentBuilder = new SAXBuilder();
            InputStream parsedMessageStream = new ByteArrayInputStream(Passed_Envelope4XSLTPost.getBytes(StandardCharsets.UTF_8));
            Document document = (Document) documentBuilder.build(parsedMessageStream); // .parse(parsedConfigStream);

            try {

                String xpathSQLStatementExpression="/SQLRequest/SQLStatement/PSTMT";
                String xpathSQLStatementParamExpression = "/SQLRequest/SQLStatement/Param[2]"; ///SQLRequest/SQLStatement/Param[2]
                Integer iMsgStaus = 1233;

                XPathExpression<Element> xpathSQLStatement = XPathFactory.instance().compile(xpathSQLStatementExpression, Filters.element());
                Element emtSQLStatement = xpathSQLStatement.evaluateFirst(document);
                if ( emtSQLStatement != null ) {
                    SQLcallableStatementExpression = emtSQLStatement.getText();
                    messageDetails.MsgReason.append("ExecuteSQLincludedXML: SQLStatement=(").append(SQLcallableStatementExpression).append(")");
                    }
                else { messageDetails.MsgReason.append("ExecuteSQLincludedXML: Не нашли ").append(xpathSQLStatementExpression).append(" в результате XSLT прообразования ").append(Passed_Envelope4XSLTPost);
                    MessegeSend_Log.error("[" + messageQueueVO.getQueue_Id() + " ] " + messageDetails.MsgReason.toString());
                    return -2;

                }

                XPathExpression<Element> xpathMessage = XPathFactory.instance().compile(xpathSQLStatementParamExpression, Filters.element());
                Element emtMessage = xpathMessage.evaluateFirst(document);
                if ( emtMessage != null ) {
                    SQLparamValue = emtMessage.getText();
                    messageDetails.MsgReason.append(" Param=").append(SQLparamValue);
                }
                else {
                    messageDetails.MsgReason.append("Не нашли ").append(xpathSQLStatementParamExpression).append(" в ").append(Passed_Envelope4XSLTPost);
                    MessegeSend_Log.error("[{} ] {}", messageQueueVO.getQueue_Id(), messageDetails.MsgReason.toString());
                    return -2;
                }

                CallableStatement enableDBMS_OUTPUTStmt;
                CallableStatement disableDBMS_OUTPUTStmt=null;
                if ( (isDebugged ) && theadDataAccess.getRdbmsVendor().equals("oracle") ) {
                    enableDBMS_OUTPUTStmt = theadDataAccess.Hermes_Connection.prepareCall("BEGIN DBMS_OUTPUT.ENABLE(buffer_size => NULL); END;");
                    disableDBMS_OUTPUTStmt = theadDataAccess.Hermes_Connection.prepareCall("BEGIN DBMS_OUTPUT.DISABLE; END;");

                    enableDBMS_OUTPUTStmt.execute();
                    enableDBMS_OUTPUTStmt.close();
                }
                try {
                    theadDataAccess.Hermes_Connection.clearWarnings();

                    // Step 2.B: Creating JDBC CallableStatement
                    callableStatement = theadDataAccess.Hermes_Connection.prepareCall (SQLcallableStatementExpression);

                    MessegeSend_Log.info("[{}] ExecuteSQLincludedXML() SQLcallableStatementExpression: {}", messageQueueVO.getQueue_Id(), SQLcallableStatementExpression);
                    // register OUT parameter
                    callableStatement.registerOutParameter(1, Types.INTEGER);
                    callableStatement.setString(2, SQLparamValue );
                    try {

                    // Step 2.C: Executing CallableStatement
                    callableStatement.execute();
                    // COMMIT! ( Мало ли кто не закоммитил )
                    theadDataAccess.Hermes_Connection.commit();
                    } catch (SQLException e) {
                        messageDetails.MsgReason.append(", SQLException callableStatement.execute():=").append(e.toString());
                        MessegeSend_Log.error("[{}] ExecuteSQLincludedXML {}", messageQueueVO.getQueue_Id(), messageDetails.MsgReason.toString());
                        callableStatement.close();
                        theadDataAccess.Hermes_Connection.rollback();
                        return -3;
                    }
                    if (isDebugged ) { // получаем отладочную информацию из SQL-function
                        SQLWarning warning = callableStatement.getWarnings();

                        while (warning != null) {
                            // System.out.println(warning.getMessage());
                            MessegeSend_Log.warn("[{} ] callableStatement.SQLWarning: {}", messageQueueVO.getQueue_Id(), warning.getMessage());
                            warning = warning.getNextWarning();
                        }
                    }
                    // get count and print in console
                    int count = callableStatement.getInt(1);
                    if (isDebugged ) {
                        MessegeSend_Log.info("[{}] ExecuteSQLincludedXML: {}.getInt return {}", messageQueueVO.getQueue_Id(), SQLcallableStatementExpression, count );
                    }
                    callableStatement.close();

                    if ((isDebugged ) && (disableDBMS_OUTPUTStmt!=null)  && theadDataAccess.getRdbmsVendor().equals("oracle")  )
                    {
                        CallableStatement getOutputStmt;
                        try {
                            getOutputStmt = theadDataAccess.Hermes_Connection.prepareCall("BEGIN DBMS_OUTPUT.GET_LINES(?, ?); END;");
                            getOutputStmt.registerOutParameter(1, Types.ARRAY, "DBMSOUTPUT_LINESARRAY");
                            getOutputStmt.registerOutParameter(2, Types.NUMERIC, 100); // For the number of lines
                            getOutputStmt.execute();

                            Array dbmsOutputArray = getOutputStmt.getArray(1);
                            String[] lines = (String[]) dbmsOutputArray.getArray();
                            for (String line : lines) {
                                MessegeSend_Log.warn("[{} ] callableStatement.dbmsOutput: {}", messageQueueVO.getQueue_Id(), line);
                            }
                            dbmsOutputArray.free();
                            getOutputStmt.close();
                            disableDBMS_OUTPUTStmt.execute();
                            disableDBMS_OUTPUTStmt.close();
                        } catch (SQLException e) {
                            disableDBMS_OUTPUTStmt.execute();
                            disableDBMS_OUTPUTStmt.close();
                        }
                    }

                    callableStatement.close();
                } catch (SQLException e) {
                    messageDetails.MsgReason.append("SQLExceptio Hermes_Connection.prepareCall:=").append(e.toString());
                    MessegeSend_Log.error("[{}] {}", messageQueueVO.getQueue_Id(), messageDetails.MsgReason.toString());
                    return -2;
                }

            } catch (Exception ex) {
                ex.printStackTrace(System.err);
                messageDetails.MsgReason.setLength(0);
                messageDetails.MsgReason.append("ExecuteSQLincludedXML.XPathFactory.xpath.evaluateFirst fault: ").append(sStackTrace.strInterruptedException(ex));

                return -1;
            }
        }catch (JDOMException | IOException ex) {
            ex.printStackTrace(System.err);
            messageDetails.MsgReason.setLength(0);
            messageDetails.MsgReason.append("ExecuteSQLincludedXML.documentBuilder fault: ").append(sStackTrace.strInterruptedException(ex));
        }

/******************** ПОТОМ
        try {
            theadDataAccess.stmtMsgQueueDet.setLong(1, messageQueueVO.getQueue_Id());
            ResultSet rs = theadDataAccess.stmtMsgQueueDet.executeQuery();
            while (rs.next()) {
                MessageDetailVO messageDetailVO = new MessageDetailVO();
                messageDetailVO.setMessageQueue(
                        rs.getString("Tag_Id"),
                        rs.getString("Tag_Value"),
                        rs.getInt("Tag_Num"),
                        rs.getInt("Tag_Par_Num")
                );
                messageDetails.Message.put(messageDetails.MessageRowNum, messageDetailVO);
                messageDetails.MessageRowNum += 1;
                // MessegeSend_Log.info( "Tag_Id:" + rs.getString("Tag_Id") + " [" + rs.getString("Tag_Value") + "]");

            }
        } catch (SQLException e) {
            MessegeSend_Log.error("Queue_Id=[" + messageQueueVO.getQueue_Id() + "] :" + sStackTrace.strInterruptedException(e));
            e.printStackTrace();
            return nn;
        }
************************************************************/
        return nn;
    }


}
