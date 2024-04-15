package net.plumbing.msgbus.model;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.HashMap;


public class MessageDetails {

    public  int MessageRowNum =0;
    public  HashMap<Integer, MessageDetailVO > Message = new HashMap<Integer, MessageDetailVO >();
    public  HashMap<Integer, ArrayList > MessageIndex_by_Tag_Par_Num = new HashMap<Integer, ArrayList>(); // для получения messageDetails.Message.get( messageDetails.RecordIndex_by_TagId.get(Tag_Num) )
    public  int ConfirmationRowNum=0;
    public  HashMap<Integer, MessageDetailVO > Confirmation = new HashMap<Integer, MessageDetailVO >();
    public StringBuilder XML_MsgOUT = new StringBuilder();
    //public StringBuilder XML_MsgSEND= new StringBuilder();
    public String XML_MsgSEND;
    public StringBuilder Soap_HeaderRequest = new StringBuilder();
    public StringBuilder XML_MsgResponse= new StringBuilder(); // ответ от сервиса в виде XML-STRING
    public StringBuilder XML_ClearBodyResponse= new StringBuilder(); // очищенный от ns: содержимое Body Response Soap
    public StringBuilder XML_MsgRESOUT= new StringBuilder(); // результат преобоазования MsgAnswXSLT ( или чистый Response)
    public StringBuilder XML_MsgConfirmation= new StringBuilder(); // <Confirmation>****</Confirmation> результат преобоазования MsgAnswXSLT ( или чистый Response),
                                                                   // который должен быть уложен в БД
                                                                    //

    public MessageTemplate4Perform MessageTemplate4Perform;
    public int MsgStatus=0;
    public StringBuilder MsgReason = new StringBuilder();
    public Integer X_Total_Count =0;
    public int ApiRestWaitTime=120;

    public int Message_Tag_Num = 0; // счетчик XML элнментов в Message
    public int Confirmation_Tag_Num = 0; // счетчик XML элнментов в Confirmation
    // public CloseableHttpClient SimpleHttpClient;
    // public java.net.http.HttpClient RestHermesAPIHttpClient;
    //public SSLContext sslContext;
    //public HttpClientBuilder httpClientBuilder;


    public  MessageDetails() {
        this.Message.clear();
        this.MessageIndex_by_Tag_Par_Num.clear();
        this.Confirmation.clear();
        this.XML_MsgOUT.setLength(0); this.XML_MsgOUT.trimToSize();
        this.XML_ClearBodyResponse.setLength(0); this.XML_ClearBodyResponse.trimToSize();
        this.XML_MsgResponse.setLength(0); this.XML_MsgResponse.trimToSize();
        this.XML_MsgRESOUT.setLength(0); this.XML_MsgRESOUT.trimToSize();
        this.MsgReason.setLength(0); this.MsgReason.trimToSize();
        this.XML_MsgConfirmation.setLength(0);
    }
    // public void SetHttpClient( CloseableHttpClient simpleHttpClient ) {
    //    this.SimpleHttpClient= simpleHttpClient;
    //}
    public void ReInitMessageDetails( int ApiRestWaitTime // SSLContext sslContext, HttpClientBuilder  httpClientBuilder , CloseableHttpClient simpleHttpClient, java.net.http.HttpClient RestHermesAPIHttpClient
    //         HttpClient создаётся всегла по месту, его бодбше не требуется инициализировать
                                    ) {
        this.Message.clear();
        this.MessageIndex_by_Tag_Par_Num.clear();
        this.Confirmation.clear();
        this.XML_MsgOUT.setLength(0); this.XML_MsgOUT.trimToSize();
        this.XML_ClearBodyResponse.setLength(0); this.XML_ClearBodyResponse.trimToSize();
        this.XML_MsgResponse.setLength(0); this.XML_MsgResponse.trimToSize();
        this.XML_MsgRESOUT.setLength(0); this.XML_MsgRESOUT.trimToSize();
        this.MsgReason.setLength(0); this.MsgReason.trimToSize();
        this.XML_MsgConfirmation.setLength(0); this.XML_MsgConfirmation.trimToSize();
        this.ApiRestWaitTime = ApiRestWaitTime;
        //this.sslContext = sslContext;
        // this.httpClientBuilder = httpClientBuilder;
        // this.SimpleHttpClient= simpleHttpClient; //  парметры соединения есть только в щаблоне
        // this.RestHermesAPIHttpClient= RestHermesAPIHttpClient; // парметры соединения есть  в щаблоне , логин и пароль, их надо учитывать при
    }
}
