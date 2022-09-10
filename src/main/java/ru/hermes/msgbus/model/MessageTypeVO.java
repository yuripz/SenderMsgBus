package ru.hermes.msgbus.model;

public class MessageTypeVO {

    private int Interface_Id;
    private int Operation_Id;
    private String Msg_Type;
    private String Msg_Type_own;
    private String Msg_TypeDesc;
    private String Msg_Direction;
    private int Msg_Handler;
    private String URL_SOAP_Send;
    private String URL_SOAP_Ack;
    private int Max_Retry_Count;
    private int Max_Retry_Time;

    public void setMessageTypeVO( int Interface_Id,
                                        int Operation_Id,
                                        String Msg_Type,
                                        String Msg_Type_own,
                                        String Msg_TypeDesc,
                                        String Msg_Direction,
                                        int Msg_Handler,
                                        String URL_SOAP_Send,
                                        String URL_SOAP_Ack,
                                        int Max_Retry_Count,
                                        int Max_Retry_Time) {
        this.Interface_Id=   Interface_Id ;
        this.Operation_Id= Operation_Id  ;
        this. Msg_Type= Msg_Type  ;
        this. Msg_Type_own= Msg_Type_own  ;
        this. Msg_TypeDesc=  Msg_TypeDesc ;
        this. Msg_Direction=  Msg_Direction  ;
        this.Msg_Handler=   Msg_Handler ;
        this. URL_SOAP_Send=  URL_SOAP_Send ;
        this. URL_SOAP_Ack=  URL_SOAP_Ack  ;
        this.Max_Retry_Count= Max_Retry_Count ;
        this.Max_Retry_Time = Max_Retry_Time;

    }

    public int getInterface_Id(){ return this.Interface_Id; }
    public int getOperation_Id(){ return this.Operation_Id; }
    public String getMsg_Type(){ return this.Msg_Type; }
    public String getMsg_Type_own(){ return this.Msg_Type_own; }
    public String getMsg_TypeDesc(){ return this.Msg_TypeDesc; }
    public String getMsg_Direction(){ return this.Msg_Direction; }
    public int getMsg_Handler(){ return this.Msg_Handler; }
    public String getURL_SOAP_Send(){ return this.URL_SOAP_Send; }
    public String getURL_SOAP_Ack(){ return this.URL_SOAP_Ack; }
    public int getMax_Retry_Count(){ return this.Max_Retry_Count; }
    public int getMax_Retry_Time(){ return this.Max_Retry_Time; }

    public void setMax_Retry_Count(int Max_Retry_Count ){  this.Max_Retry_Count=Max_Retry_Count; }
    public void setMax_Retry_Time(int Max_Retry_Time){  this.Max_Retry_Time = Max_Retry_Time; }
    public void setMsg_Type(String Msg_Type){  this.Msg_Type = Msg_Type; }
    public void setURL_SOAP_Send( String URL_SOAP_Send){  this.URL_SOAP_Send=URL_SOAP_Send; }
}
