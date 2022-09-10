package net.plumbing.msgbus.model;

public class MessageRequestVO {
    final public static String TagContext     = "Context";
    final public static String TagEventInit   = "EventInitiator";
    final public static String TagEventKey    = "EventKey";
    final public static String TagEventSrc    = "Source";
    final public static String TagEventDst    = "Destination";
    final public static String TagEventOpId   = "BusOperationId";

    final public static String TagEntryRec    = "Request";
    final public static String TagEntryInit   = "init";
    final public static String TagEntryKey    = "key";
    final public static String TagEntrySrc    = "src";
    final public static String TagEntryDst    = "dst";
    final public static String TagEntryOpId   = "opid";
    final public static String TagEntryOpName = "opname";

    final public static int  ReqTypeUnknown  = 0;
    final public static int  ReqTypeEventIn  = 1;
    final public static int  ReqTypeEventOut = 2;
    final public static int  ReqTypeConfirm  = 3;
    final public static int  ReqTypeErrTrans = 4;
    final public static int  ReqTypeIn2Out   = 5;


    protected long    eventKey=0;      //  Идентификатор события из Header
    protected String  eventInit=null;  //  Идентификатор события из Header
    protected String  destCod=null;    //  Код системы-получателя из Header
    protected String  srcCod=null;     //  Код системы-источника из Header

    protected int     operationId=0;  //  Идентификатор бизнес-операции

    protected long    requestId=0;    //  Идентификатор транзакции (ключ события)
    protected int     interfaceId=0;  //  Идентификатор интерфейса
    protected int     initId=0;       //  Идентификатор инициатора
    protected String  initSubCod="";   //  Идентификатор подсистемы
    protected int     destId=0;       //  Идентификатор приемника
    protected String  dstSubCod="";    //  Идентификатор подсистемы
    protected int     sourceId=0;     //  Идентификатор источника
    protected String  srcSubCod="";   //  Идентификатор подсистемы

    protected int     orgId=0;        //  Идентификатор операционной единицы
    protected int     errCod=0;       //  Код обработки
    protected int     reqType=ReqTypeUnknown;   // тип запроса

}
