package ru.hermes.msgbus.init;

import org.slf4j.Logger;
import ru.hermes.msgbus.common.sStackTracе;
import ru.hermes.msgbus.model.MessageDirectionsVO;
import ru.hermes.msgbus.model.MessageDirections;
import ru.hermes.msgbus.model.MessageTypeVO;
import ru.hermes.msgbus.model.MessageType;
import ru.hermes.msgbus.model.MessageTemplateVO;
import ru.hermes.msgbus.model.MessageTemplate;

import ru.hermes.msgbus.common.DataAccess;
import ru.hermes.msgbus.threads.utils.MessageRepositoryHelper;


import java.sql.*;

public class InitMessageRepository {

    private static PreparedStatement stmtMsgTypeReRead;
    private static PreparedStatement stmtMsgDirectionReRead;
    private static PreparedStatement stmtMsgTemplateReRead;
    private static final String SQLMsgDirectionReRead= "SELECT sysdate as DB_DATE, " +
            " f.msgdirection_id," +
            " f.msgdirection_cod," +
            " f.msgdirection_desc," +
            " f.app_server," +
            " f.wsdl_name," +
            " f.msgdir_own," +
            " f.operator_id," +
            " f.type_connect," +
            " f.db_name," +
            " f.db_user," +
            " f.db_pswd," +
            " f.subsys_cod," +
            " f.base_thread_id," +
            " f.num_thread," +
            " NVL(f.Short_retry_count,0) Short_retry_count, " +
            " NVL(f.Short_retry_interval,0) Short_retry_interval," +
            " NVL(f.Long_retry_count,0) Long_retry_count, " +
            " NVL(f.Long_retry_interval,0) Long_retry_interval," +
            " NVL(f.Num_Helpers_Thread,0) Num_Helpers_Thread, List_Lame_Threads" +
            " FROM ARTX_PROJ.Message_Directions F" +
            " where F.LAST_UPDATE_DT >= ?" +
            " order by f.msgdirection_iD,  f.subsys_cod,  f.msgdirection_cod";
    private static final String SQLMsgTypesReRead= "";



    public static  int ReReadMsgDirections( Long CurrentTime,
            int ShortRetryCount, int ShortRetryInterval, int LongRetryCount, int LongRetryInterval,
            Logger AppThead_log )
    {
        if ( DataAccess.Hermes_Connection == null )
        {  AppThead_log.error("DataAccess.Hermes_Connection == null");
            return -3;
        }

        ResultSet rs = null;
        Date CurrentDateTime = new Date(CurrentTime);
        try {
            AppThead_log.info(" CurrentDateTime=" +  DataAccess.dateFormat.format( CurrentDateTime ) +" :SELECT * FROM ARTX_PROJ.Message_Directions F where F.LAST_UPDATE_DT >= '" + DataAccess.dateFormat.format( DataAccess.InitDate )  +"'" );
            stmtMsgDirectionReRead.setDate(1, DataAccess.InitDate );
            rs = stmtMsgDirectionReRead.executeQuery();
            while (rs.next()) {
                // String msgdirection_cod = rs.getString("msgdirection_cod");

                int MsgDirectionVO_Key = MessageRepositoryHelper.look4MessageDirectionsVO_2_Perform( rs.getInt("msgdirection_id"),rs.getString("subsys_cod"), AppThead_log   );
                if ( MsgDirectionVO_Key < 0) {
                    MessageDirectionsVO messageDirectionsVO = new MessageDirectionsVO();
                    messageDirectionsVO.setMessageDirectionsVO(
                            rs.getInt("msgdirection_id"),
                            rs.getString("msgdirection_cod"),
                            rs.getString("Msgdirection_Desc"),
                            rs.getString("app_server"),
                            rs.getString("wsdl_name"),
                            rs.getString("msgdir_own"),
                            rs.getString("operator_id"),
                            rs.getInt("type_connect"),
                            rs.getString("db_name"),
                            rs.getString("db_user"),
                            rs.getString("db_pswd"),
                            rs.getString("subsys_cod"),
                            rs.getInt("base_thread_id"),
                            rs.getInt("num_thread"),
                            rs.getInt("short_retry_count"),
                            rs.getInt("short_retry_interval"),
                            rs.getInt("long_retry_count"),
                            rs.getInt("long_retry_interval"),
                            rs.getInt("Num_Helpers_Thread"),
                            rs.getString("List_Lame_Threads")
                    );
                    // messageDirectionsVO.LogMessageDirections( log );
                    if ( messageDirectionsVO.getShort_retry_count() == 0 )
                        messageDirectionsVO.setShort_retry_count(ShortRetryCount);

                    if ( messageDirectionsVO.getShort_retry_interval() == 0 )
                        messageDirectionsVO.setShort_retry_interval(ShortRetryInterval);

                    if ( messageDirectionsVO.getLong_retry_count() == 0 )
                        messageDirectionsVO.setLong_retry_count(LongRetryCount);

                    if ( messageDirectionsVO.getLong_retry_interval() == 0 )
                        messageDirectionsVO.setLong_retry_interval(LongRetryInterval);

                    MessageDirections.AllMessageDirections.put(MessageDirections.RowNum, messageDirectionsVO);
                    AppThead_log.info("Add system to AllMessageDirections: (MessageDirections.AllMessageDirections.size()) RowNum[" + MessageDirections.RowNum + "] =" + messageDirectionsVO.LogMessageDirections());

                    MessageDirections.RowNum += 1;
                }
                else {
                    AppThead_log.info("Update MessageDirections[" +   MsgDirectionVO_Key + "]: msgdirection_cod=" + rs.getString("msgdirection_cod") + ", "+ rs.getString("Msgdirection_Desc") );
                    MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).setWSDL_Name( rs.getString("wsdl_name"));
                    MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).setDb_pswd( rs.getString("db_pswd") );
                    MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).setDb_user(rs.getString("db_user"));
                    MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).setShort_retry_count(rs.getInt("short_retry_count"));
                    MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).setShort_retry_interval(rs.getInt("short_retry_interval"));
                    MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).setLong_retry_count(rs.getInt("long_retry_count"));
                    MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).setLong_retry_interval(rs.getInt("long_retry_interval"));
                    MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).setType_Connect(rs.getInt("type_connect"));
                    MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).setBase_Thread_Id(rs.getInt("base_thread_id"));
                    MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).setNum_Thread(rs.getInt("num_thread"));
                }
                // log.info(" MessageDirections[" +   MessageDirections.AllMessageDirections.size() + "]: longRetryInterval=" + messageDirectionsVO.getLong_retry_interval() + ", "+ messageDirectionsVO.getMsgDirection_Desc() );

            }
            rs.close();
            DataAccess.Hermes_Connection.commit();
        } catch (Exception e) {
            AppThead_log.error("ReReadMsgDirections fault: " + sStackTracе.strInterruptedException(e));
            // e.printStackTrace();
            return -2;
        }
        return MessageDirections.RowNum;
    }

    public static  int ReReadMsgTypes(Logger AppThead_log )  {
        PreparedStatement stmtMsgType = stmtMsgTypeReRead;
        int MessageTypeVOkey;
        ResultSet rs = null;

        if ( DataAccess.Hermes_Connection == null )
        {  AppThead_log.error("ReReadMsgTypes: DataAccess.Hermes_Connection == null");
            return -3;
        }

        try {
            AppThead_log.info("SELECT * FROM ARTX_PROJ.MESSAGE_typeS F where F.LAST_UPDATE_DT > '" + DataAccess.dateFormat.format( DataAccess.InitDate )  +"'" );
            stmtMsgType.setDate(1, DataAccess.InitDate );
            rs = stmtMsgType.executeQuery();
            while (rs.next()) {

                MessageTypeVOkey  = MessageRepositoryHelper.look4MessageTypeVO_2_Perform(rs.getInt("operation_id"), AppThead_log );
                if ( MessageTypeVOkey >= 0 ) {
                    MessageType.AllMessageType.get( MessageTypeVOkey ).setURL_SOAP_Send( rs.getString("url_soap_send") );
                    MessageType.AllMessageType.get( MessageTypeVOkey ).setMax_Retry_Count( rs.getInt("max_retry_count") );
                    MessageType.AllMessageType.get( MessageTypeVOkey ).setMax_Retry_Time( rs.getInt("max_retry_time"));

                }
                else {
                    MessageTypeVO messageTypeVO = new MessageTypeVO();
                    messageTypeVO.setMessageTypeVO(
                            rs.getInt("interface_id"),
                            rs.getInt("operation_id"),
                            rs.getString("msg_type"),
                            rs.getString("msg_type_own"),
                            rs.getString("msg_typedesc"),
                            rs.getString("msg_direction"),
                            rs.getInt("msg_handler"),
                            rs.getString("url_soap_send"),
                            rs.getString("url_soap_ack"),
                            rs.getInt("max_retry_count"),
                            rs.getInt("max_retry_time")
                    );
                    //messageTypeVO.LogMessageDirections( log );
                    // log.info(" messageTypeVO :", messageTypeVO );
                    // log.info(" AllMessageType.size :" +   MessageType.AllMessageType.size() );

                    MessageType.AllMessageType.put(MessageType.RowNum, messageTypeVO);
                    MessageType.RowNum += 1;
                    AppThead_log.info(" Types.size=" + MessageType.AllMessageType.size() + ", MessageRowNum[" + MessageType.RowNum + "] :" + messageTypeVO.getMsg_Type());
                }
            }
            rs.close();
            DataAccess.Hermes_Connection.commit();
            //stmtMsgType.close();
        } catch (Exception e) {
            AppThead_log.error("ReReadMsgTypes fault: " + sStackTracе.strInterruptedException(e));
            // e.printStackTrace();
            return -2;
        }
        return MessageType.RowNum;
    }

    public static  int ReReadMsgTemplates(Logger AppThead_log )  {
        int parseResult;
        int MessageTemplateVOkey;
        PreparedStatement stmtMsgTemplate = stmtMsgTemplateReRead;
        ResultSet rs;
        Logger log = AppThead_log;

        if ( DataAccess.Hermes_Connection == null )
        {  AppThead_log.error("ReReadMsgTypes: DataAccess.Hermes_Connection == null");
            return -3;
        }

        try {
            AppThead_log.info("SELECT * FROM ARTX_PROJ.MESSAGE_TemplateS T where T.LastDate > ( to_date('" + DataAccess.dateFormat.format( DataAccess.InitDate )  +"', 'YYYY-MM-DD HH24:MI:SS' ) - 2/(60*24) );" );
            stmtMsgTemplate.setDate(1, DataAccess.InitDate );
            rs = stmtMsgTemplate.executeQuery();
            while (rs.next()) {
                AppThead_log.info("ReReadMsgTemplates: Обновляем template_id[" + rs.getInt("template_id") + "]");
                MessageTemplateVOkey  = MessageRepositoryHelper.look4MessageTemplate(rs.getInt("template_id"), AppThead_log );
                if ( MessageTemplateVOkey >= 0 ) {
                    MessageTemplateVO messageTemplateVO = MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey );
                    messageTemplateVO.setInterface_Id( rs.getInt("Interface_Id") );

                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setInterface_Id( rs.getInt("Interface_Id") );
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setOperation_Id( rs.getInt("Operation_Id"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setSource_Id(rs.getInt("Source_Id"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setSrc_SubCod(rs.getString("Src_SubCod"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setDestin_Id(rs.getInt("Destin_Id"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setDst_SubCod(rs.getString("Dst_SubCod"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setMsg_Type(rs.getString("Msg_Type"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setMsg_Type_own(rs.getString("Msg_Type_own"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setTemplate_name(rs.getString("Template_name"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setTemplate_Dir(rs.getString("Template_Dir"));
//                        rs.getString("Log_Level"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setLog_Level("INFO");
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setConf_Text(rs.getString("Conf_Text"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setLastMaker(rs.getString("LastMaker"));
                    MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).setLastDate(rs.getString("LastDate"));
                    parseResult = ConfigMsgTemplates.performConfig(MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ), log);
                }
                else {

                    MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
                    messageTemplateVO.setMessageTemplateVO(
                            rs.getInt("template_id"),
                            rs.getInt("Interface_Id"),
                            rs.getInt("Operation_Id"),
                            rs.getInt("Source_Id"),
                            rs.getString("Src_SubCod"),
                            rs.getInt("Destin_Id"),
                            rs.getString("Dst_SubCod"),
                            rs.getString("Msg_Type"),
                            rs.getString("Msg_Type_own"),
                            rs.getString("Template_name"),
                            rs.getString("Template_Dir"),
//                        rs.getString("Log_Level"),
                            "INFO",
                            rs.getString("Conf_Text"),
                            rs.getString("LastMaker"),
                            rs.getString("LastDate")
                    );
                    //messageTypeVO.LogMessageDirections( log );
                    //log.info(" MessageTemplateVO :", MessageTemplateVO. );


                    // log.info(" Directions.size :" +  MessageTemplate.AllMessageTemplate.size() );

                    parseResult = ConfigMsgTemplates.performConfig(messageTemplateVO, log);
                    MessageTemplate.AllMessageTemplate.put(MessageTemplate.RowNum, messageTemplateVO);

                    log.info(" AllMessageTemplate.size :" + MessageTemplate.AllMessageTemplate.size() + " MessageRowNum =" + MessageTemplate.RowNum +
                            " Template_name:" + MessageTemplate.AllMessageTemplate.get(MessageTemplate.RowNum).getTemplate_name() + " parseConfigResult=" + parseResult);

                    MessageTemplate.RowNum += 1;
                }

            }
            rs.close();
            DataAccess.Hermes_Connection.commit();
            // stmtMsgTemplate.close();
        } catch (Exception e) {
            AppThead_log.error("ReReadMsgTemplates fault: " + sStackTracе.strInterruptedException(e));
            // e.printStackTrace();
            return -2;
        }
        return MessageTemplate.RowNum;
    }

    public static  int SelectMsgDirections(
            int ShortRetryCount, int ShortRetryInterval, int LongRetryCount, int LongRetryInterval,
            Logger AppThead_log )  {
        PreparedStatement stmtMsgDirection = null;
        ResultSet rs = null;
        Logger log = AppThead_log;

        MessageDirections.AllMessageDirections.clear();

        if ( DataAccess.Hermes_Connection != null )
            try {
                stmtMsgDirectionReRead = DataAccess.Hermes_Connection.prepareStatement(SQLMsgDirectionReRead );
                
                stmtMsgDirection = DataAccess.Hermes_Connection.prepareStatement("SELECT f.msgdirection_id," +
                        " f.msgdirection_cod," +
                        " f.msgdirection_desc," +
                        " f.app_server," +
                        " f.wsdl_name," +
                        " f.msgdir_own," +
                        " f.operator_id," +
                        " f.type_connect," +
                        " f.db_name," +
                        " f.db_user," +
                        " f.db_pswd," +
                        " f.subsys_cod," +
                        " f.base_thread_id," +
                        " f.num_thread," +
                        " NVL(f.Short_retry_count,0) Short_retry_count, " +
                        " NVL(f.Short_retry_interval,0) Short_retry_interval," +
                        " NVL(f.Long_retry_count,0) Long_retry_count, " +
                        " NVL(f.Long_retry_interval,0) Long_retry_interval," +
                        " NVL(f.Num_Helpers_Thread,0) Num_Helpers_Thread, List_Lame_Threads" +
                        " FROM ARTX_PROJ.Message_Directions F" +
                        " order by f.msgdirection_iD,  f.subsys_cod,  f.msgdirection_cod");
                
            } catch (Exception e) {
                e.printStackTrace();
                return -2;
            }
        else
        {
            return -3;
        }

        try {
            rs = stmtMsgDirection.executeQuery();
            while (rs.next()) {
                MessageDirectionsVO messageDirectionsVO = new MessageDirectionsVO();
                messageDirectionsVO.setMessageDirectionsVO(
                        rs.getInt("msgdirection_id"),
                        rs.getString("msgdirection_cod"),
                        rs.getString("Msgdirection_Desc"),
                        rs.getString("app_server"),
                        rs.getString("wsdl_name"),
                        rs.getString("msgdir_own"),
                        rs.getString("operator_id"),
                        rs.getInt("type_connect"),
                        rs.getString("db_name"),
                        rs.getString("db_user"),
                        rs.getString("db_pswd"),
                        rs.getString("subsys_cod"),
                        rs.getInt("base_thread_id"),
                        rs.getInt("num_thread"),
                        rs.getInt("short_retry_count"),
                        rs.getInt("short_retry_interval"),
                        rs.getInt("long_retry_count"),
                        rs.getInt("long_retry_interval"),
                        rs.getInt("Num_Helpers_Thread"),
                        rs.getString("List_Lame_Threads")
                );
               // messageDirectionsVO.LogMessageDirections( log );
                if (messageDirectionsVO.getShort_retry_count() == 0 )
                    messageDirectionsVO.setShort_retry_count( ShortRetryCount );

                if (messageDirectionsVO.getShort_retry_interval() == 0 )
                    messageDirectionsVO.setShort_retry_interval( ShortRetryInterval );

                if (messageDirectionsVO.getLong_retry_count() == 0 )
                    messageDirectionsVO.setLong_retry_count( LongRetryCount );

                if (messageDirectionsVO.getLong_retry_interval() == 0 )
                    messageDirectionsVO.setLong_retry_interval( LongRetryInterval );

                MessageDirections.AllMessageDirections.put( MessageDirections.RowNum, messageDirectionsVO );
                log.info( "RowNum[" + MessageDirections.RowNum + "] =" + messageDirectionsVO.LogMessageDirections() );

                MessageDirections.RowNum += 1;

                // log.info(" MessageDirections[" +   MessageDirections.AllMessageDirections.size() + "]: longRetryInterval=" + messageDirectionsVO.getLong_retry_interval() + ", "+ messageDirectionsVO.getMsgDirection_Desc() );

            }
            rs.close();
            //  stmtMsgDirectionReRead.close();
        } catch (Exception e) {
            e.printStackTrace();
            return -2;
        }
           return MessageDirections.RowNum;
    }

    public static  int SelectMsgTypes(Logger AppThead_log )  {
        PreparedStatement stmtMsgType = null;
        ResultSet rs = null;
        Logger log = AppThead_log;

        MessageType.AllMessageType.clear();
        MessageType.RowNum = 0;

        if ( DataAccess.Hermes_Connection != null )
            try {

                stmtMsgTypeReRead = DataAccess.Hermes_Connection.prepareStatement("select t.interface_id,\n" +
                            "t.operation_id,\n" +
                            "t.msg_type,\n" +
                            "t.msg_type_own,\n" +
                            "t.msg_typedesc,\n" +
                            "t.msg_direction,\n" +
                            "t.msg_handler,\n" +
                            "t.url_soap_send,\n" +
                            "t.url_soap_ack,\n" +
                            "t.max_retry_count,\n" +
                            "t.max_retry_time\n" +
                            "from ARTX_PROJ.MESSAGE_typeS t\n" +
                            "where (1=1) and t.msg_direction like '%OUT%'\n" +
                            "and t.LAST_UPDATE_DT > ?" +
                            "and t.operation_id !=0 order by t.interface_id, t.operation_id");

                stmtMsgType = DataAccess.Hermes_Connection.prepareStatement("select t.interface_id,\n" +
                        "t.operation_id,\n" +
                        "t.msg_type,\n" +
                        "t.msg_type_own,\n" +
                        "t.msg_typedesc,\n" +
                        "t.msg_direction,\n" +
                        "t.msg_handler,\n" +
                        "t.url_soap_send,\n" +
                        "t.url_soap_ack,\n" +
                        "t.max_retry_count,\n" +
                        "t.max_retry_time\n" +
                        "from ARTX_PROJ.MESSAGE_typeS t\n" +
                        "where (1=1) and t.msg_direction like '%OUT%'\n" +
                        "and t.operation_id !=0 order by t.interface_id, t.operation_id");

            } catch (Exception e) {
                e.printStackTrace();
                return -2;
            }
        else
        {
            return -3;
        }

        try {
            rs = stmtMsgType.executeQuery();
            while (rs.next()) {
                MessageTypeVO messageTypeVO = new MessageTypeVO();
                messageTypeVO.setMessageTypeVO(
                        rs.getInt("interface_id"),
                        rs.getInt("operation_id"),
                        rs.getString("msg_type"),
                        rs.getString("msg_type_own"),
                        rs.getString("msg_typedesc"),
                        rs.getString("msg_direction"),
                        rs.getInt("msg_handler"),
                        rs.getString("url_soap_send"),
                        rs.getString("url_soap_ack"),
                        rs.getInt("max_retry_count"),
                        rs.getInt("max_retry_time")
                );
                //messageTypeVO.LogMessageDirections( log );
                // log.info(" messageTypeVO :", messageTypeVO );
                // log.info(" AllMessageType.size :" +   MessageType.AllMessageType.size() );

                    MessageType.AllMessageType.put( MessageType.RowNum, messageTypeVO );
                    MessageType.RowNum += 1;

                log.info(" Types.size=" +   MessageType.AllMessageType.size() + ", MessageRowNum[" + MessageType.RowNum + "] :" + messageTypeVO.getMsg_Type() );
            }
        } catch (Exception e) {
            e.printStackTrace();
            return -2;
        }
        return MessageType.RowNum;
    }

    public static  int SelectMsgTemplates(Logger AppThead_log )  {
        int parseResult;
        PreparedStatement stmtMsgTemplate;
        ResultSet rs;
        Logger log = AppThead_log;
        //MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
        MessageTemplate.AllMessageTemplate.clear();
        MessageTemplate.RowNum = 0;

        if ( DataAccess.Hermes_Connection != null )
            try {
                stmtMsgTemplateReRead = DataAccess.Hermes_Connection.prepareStatement(
                        "select t.template_id, " +
                                "t.interface_id, " +
                                "t.operation_id, " +
                                "t.msg_type, " +
                                "t.msg_type_own, " +
                                "t.template_name, " +
                                "t.template_dir, " +
                                "t.source_id, " +
                                "t.destin_id, " +
                                "t.conf_text, " +
                                "t.src_subcod, " +
                                "t.dst_subcod, " +
                                "t.lastmaker, " +
                                "t.lastdate " +
                                "from ARTX_PROJ.MESSAGE_TemplateS t\n" +
                                "where (1=1) and t.template_dir like '%OUT%'\n" +
                                "and t.operation_id !=0 " +
                                "and t.LastDate > ( ? - 2/(60*24)) " +
                                //"and t.LastDate > to_date( ?, 'YYYY-MM-DD HH24:MI:SS) " +
                                "order by t.interface_id, t.operation_id, t.destin_id, t.dst_subcod");

                stmtMsgTemplate = DataAccess.Hermes_Connection.prepareStatement(
                        "select t.template_id, " +
                                "t.interface_id, " +
                                "t.operation_id, " +
                                "t.msg_type, " +
                                "t.msg_type_own, " +
                                "t.template_name, " +
                                "t.template_dir, " +
                                "t.source_id, " +
                                "t.destin_id, " +
                                "t.conf_text, " +
                                "t.src_subcod, " +
                                "t.dst_subcod, " +
                                "t.lastmaker, " +
                                "t.lastdate " +
                        "from ARTX_PROJ.MESSAGE_TemplateS t\n" +
                        "where (1=1) and t.template_dir like '%OUT%'\n" +
                        "and t.operation_id !=0 " +
                                "order by t.interface_id, t.operation_id, t.destin_id, t.dst_subcod");

            } catch (Exception e) {
                e.printStackTrace();
                return -2;
            }
        else
        {
            return -3;
        }

        try {
            rs = stmtMsgTemplate.executeQuery();
            while (rs.next()) {
                MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
                messageTemplateVO.setMessageTemplateVO(
                        rs.getInt("template_id"),
                        rs.getInt("Interface_Id"),
                        rs.getInt("Operation_Id"),
                        rs.getInt("Source_Id"),
                        rs.getString("Src_SubCod"),
                        rs.getInt("Destin_Id"),
                        rs.getString("Dst_SubCod"),
                        rs.getString("Msg_Type"),
                        rs.getString("Msg_Type_own"),
                        rs.getString("Template_name"),
                        rs.getString("Template_Dir"),
//                        rs.getString("Log_Level"),
                        "INFO",
                        rs.getString("Conf_Text"),
                        rs.getString("LastMaker"),
                        rs.getString("LastDate")
                );
                //messageTypeVO.LogMessageDirections( log );
                //log.info(" MessageTemplateVO :", MessageTemplateVO. );


                // log.info(" Directions.size :" +  MessageTemplate.AllMessageTemplate.size() );

                parseResult = ConfigMsgTemplates.performConfig(messageTemplateVO, log);
                MessageTemplate.AllMessageTemplate.put(MessageTemplate.RowNum, messageTemplateVO);

                log.info(" AllMessageTemplate.size :" + MessageTemplate.AllMessageTemplate.size() + " MessageRowNum =" + MessageTemplate.RowNum +
                        " Template_name:" + MessageTemplate.AllMessageTemplate.get(MessageTemplate.RowNum).getTemplate_name() +" parseConfigResult=" + parseResult);

                MessageTemplate.RowNum += 1;


            }
        } catch (Exception e) {
            e.printStackTrace();
            return -2;
        }
        return MessageTemplate.RowNum;
    }


}