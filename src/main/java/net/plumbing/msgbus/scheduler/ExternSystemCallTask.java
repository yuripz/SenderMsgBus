package net.plumbing.msgbus.scheduler;

import net.plumbing.msgbus.common.sStackTracе;
import net.plumbing.msgbus.model.*;
import net.plumbing.msgbus.threads.TheadDataAccess;
import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;
import net.plumbing.msgbus.threads.utils.MessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
//import org.springframework.context.annotation.Bean;
//import org.springframework.scheduling.annotation.EnableScheduling;
//import org.springframework.stereotype.Component;

//@Component
//@EnableScheduling

record Template_4_MessageType( int MsgDirection_Id, String Subsys_Cod) { }

public class ExternSystemCallTask implements Runnable {
    private Integer WaitTimeBetweenScan=90;
    private Integer theadNum=null;
    private String MessageType_4_Scheduler;
    private String HrmsSchema;
    private String HrmsPoint;
    private String hrmsDbLogin;
    private String hrmsDbPasswd;
    // record Template_4_MessageType(String MsgDirection_Cod, int MsgDirection_Id, String Subsys_Cod) { }

    public void setWaitTimeBetweenScan(Integer waitTimeBetweenScan) {
        this.WaitTimeBetweenScan = waitTimeBetweenScan;
    }
    public void setMessageType_4_Scheduled( String messageType_4_Scheduler) { this.MessageType_4_Scheduler = messageType_4_Scheduler; }
    public static final Logger externSystemCallTas_Log = LoggerFactory.getLogger(ExternSystemCallTask.class);
    public void setTheadNum( int TheadNum) {
        this.theadNum = TheadNum;
    }

    public void setHrmsPoint(String hrmspoint) {
        this.HrmsPoint = hrmspoint;
    }
    public void setHrmsSchema( String HrmsSchema) { this.HrmsSchema = HrmsSchema; }
    public void setHrmsDbPasswd(String hrmsDbPasswd) {
        this.hrmsDbPasswd = hrmsDbPasswd;
    }
    public void setHrmsDbLogin(String hrmsDbLogin) {
        this.hrmsDbLogin = hrmsDbLogin;
    }


    private int theadRunCount = 0;
    private  int  theadRunTotalCount = 1;

//    @Bean( name = "ExternSystemCallTaskRun")
    public void run() {
        externSystemCallTas_Log.info("ExternSystemCallTask[" + theadNum + "]: is started with interval=" + WaitTimeBetweenScan);

       if ( theadNum == null ) return;
        int Operation_Id_4_Scheduled = MessageRepositoryHelper.getOperation_Id_by_MesssageType( MessageType_4_Scheduler, externSystemCallTas_Log );
        if ( Operation_Id_4_Scheduled < 0) {
            externSystemCallTas_Log.error("НЕ удалось найти тип сообщения по его типу=`" + MessageType_4_Scheduler + "`");
            return;
        }
       //  if ( theadNum != null ) return;

        // TheadDataAccess
        TheadDataAccess theadDataAccess = new TheadDataAccess();
        theadDataAccess.setDbSchema( HrmsSchema );
        // Установаливем " соединение" , что бы зачитывать очередь
        Connection Hermes_Connection = theadDataAccess.make_Hermes_Connection(  HrmsPoint, hrmsDbLogin, hrmsDbPasswd, 100000 + theadNum,
                externSystemCallTas_Log
        );
        if ( Hermes_Connection == null) {
            externSystemCallTas_Log.error("НЕ удалось Установить соединение , что бы подготовить запросы к БД ");
            return;
        }

        MessageQueueVO messageQueueVO = new MessageQueueVO();

        // HashMap<Integer, Template_4_MessageType> TemplateList_4_MessageType = new HashMap<Integer, Template_4_MessageType>();

        ArrayList<Template_4_MessageType> TemplateList_4_MessageType = new ArrayList<Template_4_MessageType>();
        int MessageTemplateVOkey=-1;

        for (int i = 0; i < MessageTemplate.AllMessageTemplate.size(); i++) {
            MessageTemplateVO messageTemplateVO = MessageTemplate.AllMessageTemplate.get( i );
            String Msg_Type = messageTemplateVO.getMsg_Type();
            String Template_Dir = messageTemplateVO.getTemplate_Dir();

            if ((Msg_Type.equalsIgnoreCase(MessageType_4_Scheduler ) ) && (Template_Dir.equals("OUT"))) {
                // Msg_Type Шаблонов совпали,  Template_Id = i;
                MessageTemplateVOkey = i;
                int MsgDirection_Id = messageTemplateVO.getDestin_Id();
                if ( MsgDirection_Id > 0) { // Шаблон реально предназначен для конкретно внешнией системы
                    Template_4_MessageType template_4_MessageType_add = new Template_4_MessageType( MsgDirection_Id, messageTemplateVO.getDst_SubCod());
                    TemplateList_4_MessageType.add(template_4_MessageType_add);
                    externSystemCallTas_Log.info( "look4MessageTemplate: добавили в список систем для отправки [" + MessageTemplateVOkey +"]: Template_Id=" + MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getTemplate_Id());
                }
            }
        }
        if ( TemplateList_4_MessageType.size() == 0 ) {
            externSystemCallTas_Log.info("look4MessageTemplate, получаем TemplateList_4_MessageType.size()==0: значит, не нашли ни одного шаблона для `" + MessageType_4_Scheduler + "`");
        }

        for ( theadRunCount = 0 ; true; theadRunCount += 1 )
        try {
            externSystemCallTas_Log.info("ExternSystemCallTask[" + theadNum + "]: is runing [" + theadRunCount + "] times 4 make Operation_Id=" + Operation_Id_4_Scheduled);

            for( int i=0; i< TemplateList_4_MessageType.size(); i++ ) {
                Template_4_MessageType template_4_MakeNewMessage_Queue = TemplateList_4_MessageType.get(i);
                // Создаем запись в таблице-очереди  select ARTX_PROJ.MESSAGE_QUEUE_SEQ.NEXTVAL ...
                Long Queue_Id = MessageUtils.MakeNewMessage_Queue(messageQueueVO, theadDataAccess, externSystemCallTas_Log);
                if (Queue_Id == null) {
                    externSystemCallTas_Log.error("Ошибка на на создании сообщения, не удалось сохранить заголовок сообщения в БД - MakeNewMessage_Queue() return null , " + MessageType_4_Scheduler);
                }
                else {
                    // Получить Msg_InfoStreamId из вызова
                    // x_message$_get_thread_num(p_msgdirection_id integer, p_subsys_cod character varying, p_operation_id integer, p_msg_infostreamid integer, p_queue_id bigint)

                    messageQueueVO.setOperation_Id(Operation_Id_4_Scheduled);
                    messageQueueVO.setMsgDirection_Id(template_4_MakeNewMessage_Queue.MsgDirection_Id());
                    messageQueueVO.setSubSys_Cod(template_4_MakeNewMessage_Queue.Subsys_Cod());
                    int MsgDirectionVO_Key = MessageRepositoryHelper.look4MessageDirectionsVO_2_Perform(template_4_MakeNewMessage_Queue.MsgDirection_Id(), template_4_MakeNewMessage_Queue.Subsys_Cod(), externSystemCallTas_Log);
                    int Msg_InfoStreamId = MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getBase_Thread_Id();
                    int Num_Thread = MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getNum_Thread();
                    if (MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getNum_Thread() < 1)
                        Num_Thread = 1;
                    // v_Thread_Id := v_base_thread_id + mod( p_queue_id, v_num_thread );
                    if (Msg_InfoStreamId > 100) {
                        Msg_InfoStreamId = Msg_InfoStreamId + (int) (Queue_Id % Num_Thread);
                        messageQueueVO.setMsg_InfoStreamId(Msg_InfoStreamId);
                    } else
                        messageQueueVO.setMsg_InfoStreamId(101);

                    // insert into  message_queuedet <MessageType_4_Scheduler><Parametrs><Queue_Id>12345</Queue_Id></Parametrs></MessageType_4_Scheduler>
                    try {

                    theadDataAccess.stmt_INSERT_Message_Details.setLong(1, Queue_Id);
                    theadDataAccess.stmt_INSERT_Message_Details.setString(2, MessageType_4_Scheduler); // TAG_ID
                    theadDataAccess.stmt_INSERT_Message_Details.setString(3, null); // TAG_VALUE
                    theadDataAccess.stmt_INSERT_Message_Details.setInt(4, 1); // TAG_NUM
                    theadDataAccess.stmt_INSERT_Message_Details.setInt(5, 0); // TAG_PAR_NUM
                    theadDataAccess.stmt_INSERT_Message_Details.addBatch();

                        theadDataAccess.stmt_INSERT_Message_Details.setLong(1, Queue_Id);
                        theadDataAccess.stmt_INSERT_Message_Details.setString(2, "Parametrs");
                        theadDataAccess.stmt_INSERT_Message_Details.setString(3, null);
                        theadDataAccess.stmt_INSERT_Message_Details.setInt(4, 2); // TAG_NUM
                        theadDataAccess.stmt_INSERT_Message_Details.setInt(5, 1); // TAG_PAR_NUM
                        theadDataAccess.stmt_INSERT_Message_Details.addBatch();

                        theadDataAccess.stmt_INSERT_Message_Details.setLong(1, Queue_Id);
                        theadDataAccess.stmt_INSERT_Message_Details.setString(2, "Queue_Id");
                        theadDataAccess.stmt_INSERT_Message_Details.setString(3, Queue_Id.toString());
                        theadDataAccess.stmt_INSERT_Message_Details.setInt(4, 3); // TAG_NUM
                        theadDataAccess.stmt_INSERT_Message_Details.setInt(5, 2); // TAG_PAR_NUM
                        theadDataAccess.stmt_INSERT_Message_Details.addBatch();

                        // Insert data in Oracle with Java … Batched mode
                        theadDataAccess.stmt_INSERT_Message_Details.executeBatch();

                    theadDataAccess.doUPDATE_MessageQueue_OUT2Ok(Queue_Id, Operation_Id_4_Scheduled, Msg_InfoStreamId,
                            messageQueueVO.getMsgDirection_Id(), messageQueueVO.getSubSys_Cod(),
                            MessageType_4_Scheduler, MessageType_4_Scheduler, "", Queue_Id.toString(), externSystemCallTas_Log);
                    } catch (SQLException e) {
                        externSystemCallTas_Log.error(theadDataAccess.INSERT_Message_Details + ":Queue_Id=[" + Queue_Id + "] :" + sStackTracе.strInterruptedException(e));
                        e.printStackTrace();
                        try {
                            theadDataAccess.Hermes_Connection.rollback();
                        } catch (SQLException exp) {
                            externSystemCallTas_Log.error("Hermes_Connection.rollback()fault: " + exp.getMessage());
                        }

                    }
                }
            }

            Thread.sleep( 1000L * WaitTimeBetweenScan );

        } catch (InterruptedException e) {
            externSystemCallTas_Log.error("ExternSystemCallTask[" + theadNum + "]: is interrapted: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
