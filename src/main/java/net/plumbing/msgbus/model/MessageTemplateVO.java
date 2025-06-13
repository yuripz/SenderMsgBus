package net.plumbing.msgbus.model;

import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.sStackTrace;
import net.sf.saxon.s9api.*;
import org.slf4j.Logger;

import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class MessageTemplateVO {


    private int Template_Id;
    private int Interface_Id;
    private int Operation_Id;
    private int Source_Id;
    private String Src_SubCod;
    private int Destin_Id;
    private String Dst_SubCod;
    private String Msg_Type;
    private String Msg_Type_own;
    private String Template_name;
    private String Template_Dir;
    private String Log_Level;
    private String Conf_Text;
    private String LastMaker;
    private String LastDate;

    private String EnvelopeNS;
    private String ConfigExecute;
    private String ConfigPostExec;
    private String MessageXSD;
    
    private String HeaderXSLT;
    private String MessageXSLT;
    private String EnvelopeXSLTExt;

    private String EnvelopeXSLTPost;

    private String MsgAnswXSLT;
    private String AckXSLT;
    private String ErrTransXSLT;
    
    /* no Usage    
        private String fixMessageNS;
        private String MessageAck;
        private String MessageAnswAck;
        private String MessageAnswerXSD;
        private String MessageAnswMsgXSLT;
        private String MessageAnswHdXSLT;
        private String AckXSD;
        private String AnswAckXSLT;
        private String AnswAckHdXSLT;
        private String ErrTransXSD;
        private String HeaderXSD;
        private String MessageXSLTNew;        
    
        private String PropHost;
        private String PropUser;
        private String PropPswd;
        private String PropUrl;
        private String PropEncoding_Out;
        private String PropEncoding_In;
        private String PropTimeout_Conn;
        private String PropTimeout_Read;
        private String PropMaxRetryCnt;
        private String PropMaxRetryTime;
    
        public static final String  PropNameWebHost  = "host";
        public static final String  PropNameWebUser  = "user";
        public static final String  PropNameWebPswd  = "pswd";
        public static final String  PropNameWebUrl   = "url";
        public static final String  PropNameCharOut  = "encoding_out";
        public static final String  PropNameCharIn   = "encoding_in";
        public static final String  PropNameConnTimeOut = "timeout_conn";
        public static final String  PropNameReadTimeOut = "timeout_read";
        public static final String  PropNameMaxRetryCnt  = "max_retry_count";
        public static final String  PropNameMaxRetryTime = "max_retry_minut";
    
        public static final String  PropNameJavaClass     = "class";
        public static final String  PropNameJavaJndiConn  = "jndi_conn";
        public static final String  PropNameShName        = "script";
        public static final String  PropNameParamPref     = "ParamList";
    */
    // for pre-compile XSLT at ConfigMsgTemplates.performConfig( MessageTemplateVO messageTemplateVO, Logger AppThead_log) 
    // HeaderXSLT
    private Processor HeaderXSLT_processor; // = new Processor(false);
    private XsltCompiler HeaderXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private XsltExecutable HeaderXSLT_xsltStylesheet;
    private Xslt30Transformer HeaderXSLT_xslt30Transformer;
    public Xslt30Transformer getHeaderXSLT_xslt30Transformer() {return HeaderXSLT_xslt30Transformer;}
    public Processor getHeaderXSLT_processor() {return HeaderXSLT_processor;}
    public XsltCompiler getHeaderXSLT_xsltCompiler() {return HeaderXSLT_xsltCompiler;}

    public void makeHeaderXSLT_Transformer( Logger AppThead_log )   {
        StreamSource srcXSLT;
        if ( ! HeaderXSLT.isEmpty() ) // если не пустой XSLT-текст
        {
            try {
                srcXSLT = new StreamSource(new ByteArrayInputStream(HeaderXSLT.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeHeaderXSLT_Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") ByteArrayInputStream Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeHeaderXSLT_Transformer for Msg_Type `{}` ({}|{}) ByteArrayInputStream Exception: {}", Msg_Type, Interface_Id, Operation_Id, ConvXMLuseXSLTerr);

                return;
            }
            try {
                HeaderXSLT_processor = new Processor(false);
                HeaderXSLT_xsltCompiler = HeaderXSLT_processor.newXsltCompiler();
                HeaderXSLT_xsltStylesheet = HeaderXSLT_xsltCompiler.compile(srcXSLT);
            } catch (SaxonApiException exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeHeaderXSLT_Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") xsltCompiler Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeHeaderXSLT_Transformer for Msg_Type `{}` ({}|{}) xsltCompiler of `{}` Exception: {}", Msg_Type, Interface_Id, Operation_Id, HeaderXSLT, ConvXMLuseXSLTerr);
                return;
            }
            HeaderXSLT_xslt30Transformer = HeaderXSLT_xsltStylesheet.load30();
            return;
        }
    }
    
    // MsgAnswXSLT
    private Processor MsgAnswXSLT_processor; // = new Processor(false);
    private XsltCompiler MsgAnswXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private XsltExecutable MsgAnswXSLT_xsltStylesheet;
    private Xslt30Transformer MsgAnswXSLT_xslt30Transformer;
    public Xslt30Transformer getMsgAnswXSLT_xslt30Transformer() {return MsgAnswXSLT_xslt30Transformer;}
    public Processor getMsgAnswXSLT_processor() {return MsgAnswXSLT_processor;}
    public XsltCompiler getMsgAnswXSLT_xsltCompiler() {return MsgAnswXSLT_xsltCompiler;}
    public void makeMsgAnswXSLT_xslt30Transformer( Logger AppThead_log )   {
        StreamSource srcXSLT;
        if ( ! MsgAnswXSLT.isEmpty() ) // если не пустой XSLT-текст
        {
            try {
                srcXSLT = new StreamSource(new ByteArrayInputStream(MsgAnswXSLT.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeMsgAnswXSLT_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") ByteArrayInputStream Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeMsgAnswXSLT_xslt30Transformer for Msg_Type `{}` ({}|{}) ByteArrayInputStream Exception: {}", Msg_Type, Interface_Id, Operation_Id, ConvXMLuseXSLTerr);

                return;
            }
            try {
                MsgAnswXSLT_processor = new Processor(false);
                MsgAnswXSLT_xsltCompiler = MsgAnswXSLT_processor.newXsltCompiler();
                MsgAnswXSLT_xsltStylesheet = MsgAnswXSLT_xsltCompiler.compile(srcXSLT);
            } catch (SaxonApiException exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeMsgAnswXSLT_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") xsltCompiler Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeMsgAnswXSLT_xslt30Transformer for Msg_Type `{}` ({}|{}) xsltCompiler of `{}` Exception: {}", Msg_Type, Interface_Id, Operation_Id, MsgAnswXSLT, ConvXMLuseXSLTerr);
                return;
            }
            MsgAnswXSLT_xslt30Transformer = MsgAnswXSLT_xsltStylesheet.load30();
            return;
        }
    }
    
    // MessageXSLT
    private Processor MessageXSLT_processor; // = new Processor(false);
    private XsltCompiler MessageXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private XsltExecutable MessageXSLT_xsltStylesheet;
    private Xslt30Transformer MessageXSLT_xslt30Transformer;
    public Xslt30Transformer getMessageXSLT_xslt30Transformer() {return MessageXSLT_xslt30Transformer;}
    public Processor getMessageXSLT_processor() {return MessageXSLT_processor;}
    public XsltCompiler getMessageXSLT_xsltCompiler() {return MessageXSLT_xsltCompiler;}
    public void makeMessageXSLT_xslt30Transformer( Logger AppThead_log )   {
        StreamSource srcXSLT;
        if ( ! MessageXSLT.isEmpty() ) // если не пустой XSLT-текст
        {
            try {
                srcXSLT = new StreamSource(new ByteArrayInputStream(MessageXSLT.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeMessageXSLT_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") ByteArrayInputStream Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeMessageXSLT_xslt30Transformer for Msg_Type `{}` ({}|{}) ByteArrayInputStream Exception: {}", Msg_Type, Interface_Id, Operation_Id, ConvXMLuseXSLTerr);

                return;
            }
            try {
                MessageXSLT_processor = new Processor(false);
                MessageXSLT_xsltCompiler = MessageXSLT_processor.newXsltCompiler();
                MessageXSLT_xsltStylesheet = MessageXSLT_xsltCompiler.compile(srcXSLT);
            } catch (SaxonApiException exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeMessageXSLT_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") xsltCompiler Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeMessageXSLT_xslt30Transformer for Msg_Type `{}` ({}|{}) xsltCompiler of `{}` Exception: {}", Msg_Type, Interface_Id, Operation_Id, MessageXSLT, ConvXMLuseXSLTerr);
                return;
            }
            MessageXSLT_xslt30Transformer = MessageXSLT_xsltStylesheet.load30();
            return;
        }
    }
    
    // AckXSLT
    private Processor AckXSLT_processor; // = new Processor(false);
    private XsltCompiler AckXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private XsltExecutable AckXSLT_xsltStylesheet;
    private Xslt30Transformer AckXSLT_xslt30Transformer;
    public Xslt30Transformer getAckXSLT_xslt30Transformer() {return AckXSLT_xslt30Transformer;}
    public Processor getMAckXSLT_processor() {return AckXSLT_processor;}
    public XsltCompiler getAckXSLT_xsltCompiler() {return AckXSLT_xsltCompiler;}
    public void makeAckXSLT_xslt30Transformer( Logger AppThead_log )   {
        StreamSource srcXSLT;
        if ( ! AckXSLT.isEmpty() ) // если не пустой XSLT-текст
        {
            try {
                srcXSLT = new StreamSource(new ByteArrayInputStream(AckXSLT.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeAckXSLT_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") ByteArrayInputStream Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeAckXSLT_xslt30Transformer for Msg_Type `{}` ({}|{}) ByteArrayInputStream Exception: {}", Msg_Type, Interface_Id, Operation_Id, ConvXMLuseXSLTerr);

                return;
            }
            try {
                AckXSLT_processor = new Processor(false);
                AckXSLT_xsltCompiler = AckXSLT_processor.newXsltCompiler();
                AckXSLT_xsltStylesheet = AckXSLT_xsltCompiler.compile(srcXSLT);
            } catch (SaxonApiException exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeAckXSLT_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") xsltCompiler Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeAckXSLT_xslt30Transformer for Msg_Type `{}` ({}|{}) xsltCompiler of `{}` Exception: {}", Msg_Type, Interface_Id, Operation_Id, AckXSLT, ConvXMLuseXSLTerr);
                return;
            }
            AckXSLT_xslt30Transformer = AckXSLT_xsltStylesheet.load30();
            return;
        }
    }
    
    // EnvelopeXSLTPost
    private Processor EnvelopeXSLTPost_processor; // = new Processor(false);
    private XsltCompiler EnvelopeXSLTPost_xsltCompiler; // = processor.newXsltCompiler();
    private XsltExecutable EnvelopeXSLTPost_xsltStylesheet;
    private Xslt30Transformer EnvelopeXSLTPost_xslt30Transformer;
    public Xslt30Transformer getEnvelopeXSLTPost_xslt30Transformer() {return EnvelopeXSLTPost_xslt30Transformer;}
    public Processor getEnvelopeXSLTPost_processor() {return EnvelopeXSLTPost_processor;}
    public XsltCompiler getEnvelopeXSLTPost_xsltCompiler() {return EnvelopeXSLTPost_xsltCompiler;}
    public void makeEnvelopeXSLTPost_xslt30Transformer( Logger AppThead_log )   {
        StreamSource srcXSLT;
        if ( ! EnvelopeXSLTPost.isEmpty() ) // если не пустой XSLT-текст
        {
            try {
                srcXSLT = new StreamSource(new ByteArrayInputStream(EnvelopeXSLTPost.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeEnvelopeXSLTPost_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") ByteArrayInputStream Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeEnvelopeXSLTPost_xslt30Transformer for Msg_Type `{}` ({}|{}) ByteArrayInputStream Exception: {}", Msg_Type, Interface_Id, Operation_Id, ConvXMLuseXSLTerr);

                return;
            }
            try {
                EnvelopeXSLTPost_processor = new Processor(false);
                EnvelopeXSLTPost_xsltCompiler = EnvelopeXSLTPost_processor.newXsltCompiler();
                EnvelopeXSLTPost_xsltStylesheet = EnvelopeXSLTPost_xsltCompiler.compile(srcXSLT);
            } catch (SaxonApiException exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeEnvelopeXSLTPost_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") xsltCompiler Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeEnvelopeXSLTPost_xslt30Transformer for Msg_Type `{}` ({}|{}) xsltCompiler of `{}` Exception: {}", Msg_Type, Interface_Id, Operation_Id, EnvelopeXSLTPost, ConvXMLuseXSLTerr);
                return;
            }
            EnvelopeXSLTPost_xslt30Transformer = EnvelopeXSLTPost_xsltStylesheet.load30();
            return;
        }
    }
    
    // EnvelopeXSLTExt
    private Processor EnvelopeXSLTExt_processor; // = new Processor(false);
    private XsltCompiler EnvelopeXSLTExt_xsltCompiler; // = processor.newXsltCompiler();
    private XsltExecutable EnvelopeXSLTExt_xsltStylesheet;
    private Xslt30Transformer EnvelopeXSLTExt_xslt30Transformer;
    public Xslt30Transformer getEnvelopeXSLTExt_xslt30Transformer() {return EnvelopeXSLTExt_xslt30Transformer;}
    public Processor getEnvelopeXSLTExt_processor() {return EnvelopeXSLTExt_processor;}
    public XsltCompiler getEnvelopeXSLTExt_xsltCompiler() {return EnvelopeXSLTExt_xsltCompiler;}
    public void makeEnvelopeXSLTExt_xslt30Transformer( Logger AppThead_log )   {
        StreamSource srcXSLT;
        if ( ! EnvelopeXSLTExt.isEmpty() ) // если не пустой XSLT-текст
        {
            try {
                srcXSLT = new StreamSource(new ByteArrayInputStream(EnvelopeXSLTExt.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeEnvelopeXSLTExt_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") ByteArrayInputStream Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeEnvelopeXSLTExt_xslt30Transformer for Msg_Type `{}` ({}|{}) ByteArrayInputStream Exception: {}", Msg_Type, Interface_Id, Operation_Id, ConvXMLuseXSLTerr);

                return;
            }
            try {
                EnvelopeXSLTExt_processor = new Processor(false);
                EnvelopeXSLTExt_xsltCompiler = EnvelopeXSLTExt_processor.newXsltCompiler();
                EnvelopeXSLTExt_xsltStylesheet = EnvelopeXSLTExt_xsltCompiler.compile(srcXSLT);
            } catch (SaxonApiException exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeEnvelopeXSLTExt_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") xsltCompiler Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeEnvelopeXSLTExt_xslt30Transformer for Msg_Type `{}` ({}|{}) xsltCompiler of `{}` Exception: {}", Msg_Type, Interface_Id, Operation_Id, EnvelopeXSLTExt, ConvXMLuseXSLTerr);
                return;
            }
            EnvelopeXSLTExt_xslt30Transformer = EnvelopeXSLTExt_xsltStylesheet.load30();
            return;
        }
    }

    
    // ErrTransXSLT
    private Processor ErrTransXSLT_processor; // = new Processor(false);
    private XsltCompiler ErrTransXSLT_xsltCompiler; // = processor.newXsltCompiler();
    private XsltExecutable ErrTransXSLT_xsltStylesheet;
    private Xslt30Transformer ErrTransXSLT_xslt30Transformer;
    public Xslt30Transformer getErrTransXSLT_xslt30Transformer() {return ErrTransXSLT_xslt30Transformer;}
    public Processor getErrTransXSLT_processor() {return ErrTransXSLT_processor;}
    public XsltCompiler getErrTransXSLT_xsltCompiler() {return ErrTransXSLT_xsltCompiler;}
    public void makeErrTransXSLT_xslt30Transformer( Logger AppThead_log )   {
        StreamSource srcXSLT;
        if ( ! ErrTransXSLT.isEmpty() ) // если не пустой XSLT-текст
        {
            try {
                srcXSLT = new StreamSource(new ByteArrayInputStream(ErrTransXSLT.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeErrTransXSLT_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") ByteArrayInputStream Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeErrTransXSLT_xslt30Transformer for Msg_Type `{}` ({}|{}) ByteArrayInputStream Exception: {}", Msg_Type, Interface_Id, Operation_Id, ConvXMLuseXSLTerr);

                return;
            }
            try {
                ErrTransXSLT_processor = new Processor(false);
                ErrTransXSLT_xsltCompiler = ErrTransXSLT_processor.newXsltCompiler();
                ErrTransXSLT_xsltStylesheet = ErrTransXSLT_xsltCompiler.compile(srcXSLT);
            } catch (SaxonApiException exp) {
                String ConvXMLuseXSLTerr = sStackTrace.strInterruptedException(exp);
                System.err.println("makeErrTransXSLT_xslt30Transformer for Msg_Type `" + Msg_Type + "` (" + Interface_Id + "|" + Operation_Id + ") xsltCompiler Exception:");
                exp.printStackTrace();
                AppThead_log.error("makeErrTransXSLT_xslt30Transformer for Msg_Type `{}` ({}|{}) xsltCompiler of `{}` Exception: {}", Msg_Type, Interface_Id, Operation_Id, ErrTransXSLT, ConvXMLuseXSLTerr);
                return;
            }
            ErrTransXSLT_xslt30Transformer = ErrTransXSLT_xsltStylesheet.load30();
            return;
        }
    }

    public void setMessageTemplateVO(
            int Template_Id,
            int Interface_Id,
            int Operation_Id,
            int Source_Id,
            String Src_SubCod,
            int Destin_Id,
            String Dst_SubCod,
            String Msg_Type,
            String Msg_Type_own,
            String Template_name,
            String Template_Dir,
            String Log_Level,
            String Conf_Text,
            String LastMaker,
            String LastDate) {

        HeaderXSLT_xslt30Transformer = null;
        MsgAnswXSLT_xslt30Transformer = null;
        AckXSLT_xslt30Transformer = null;
        MessageXSLT_xslt30Transformer =null;
        EnvelopeXSLTPost_xslt30Transformer = null;
        EnvelopeXSLTExt_xslt30Transformer = null;
        ErrTransXSLT_xslt30Transformer = null;

        this.Template_Id = Template_Id ;
        this.Interface_Id = Interface_Id ;
        this.Operation_Id = Operation_Id ;
        this.Source_Id = Source_Id ;
        this.Src_SubCod = Src_SubCod ;
        this.Destin_Id = Destin_Id ;
        this.Dst_SubCod = Dst_SubCod ;
        this.Msg_Type = Msg_Type ;
        this.Msg_Type_own = Msg_Type_own ;
        this.Template_name = Template_name ;
        this.Template_Dir = Template_Dir ;
        this.Log_Level = Log_Level ;
        this.Conf_Text = Conf_Text ;
        this.LastMaker = LastMaker ;
        this.LastDate = LastDate ;


    }
    public int getTemplate_Id(){ return this.Template_Id;  }
    public int getInterface_Id(){ return this.Interface_Id;  }
    public int getOperation_Id(){ return this.Operation_Id ;  }
    public int getSource_Id(){ return this.Source_Id ;  }
    public String getSrc_SubCod(){ return this.Src_SubCod;  }
    public int getDestin_Id(){ return this.Destin_Id;  }
    public String getDst_SubCod(){ return this.Dst_SubCod;  }
    public String getMsg_Type(){ return this.Msg_Type;  }
    public String getMsg_Type_own(){ return this.Msg_Type_own;  }
    public String getTemplate_name(){ return this.Template_name;  }
    public String getTemplate_Dir(){ return this.Template_Dir;  }
    public String getLog_Level(){ return this.Log_Level;  }
    public String getConf_Text(){ return this.Conf_Text;  }
    public String getLastMaker(){ return this.LastMaker;  }
    public String getLastDate(){ return this.LastDate;  }


    public void setTemplate_Id(int Template_Id){ this.Template_Id = Template_Id;  }
    public void setInterface_Id(int Interface_Id){  this.Interface_Id =Interface_Id;  }
    public void setOperation_Id(int Operation_Id){ this.Operation_Id = Operation_Id ;  }
    public void setSource_Id(int Source_Id){  this.Source_Id = Source_Id ;  }
    public void setSrc_SubCod(String Src_SubCod){  this.Src_SubCod = Src_SubCod;  }
    public void setDestin_Id(int Destin_Id){  this.Destin_Id = Destin_Id;  }
    public void setDst_SubCod(String Dst_SubCod){  this.Dst_SubCod = Dst_SubCod;  }
    public void setMsg_Type(String Msg_Type){  this.Msg_Type = Msg_Type;  }
    public void setMsg_Type_own(String Msg_Type_own){  this.Msg_Type_own = Msg_Type_own;  }
    public void setTemplate_name(String Template_name){  this.Template_name = Template_name;  }
    public void setTemplate_Dir(String Template_Dir){  this.Template_Dir = Template_Dir;  }
    public void setLog_Level(String Log_Level){  this.Log_Level = Log_Level;  }
    public void setConf_Text(String Conf_Text){  this.Conf_Text = Conf_Text;  }
    public void setLastMaker(String LastMaker){  this.LastMaker = LastMaker;  }
    public void setLastDate(String LastDate){  this.LastDate = LastDate;  }


    public void setConfigExecute (String ConfigExecute) { this.ConfigExecute = ConfigExecute; }
    public String getConfigExecute() { return this.ConfigExecute; }

    public void setConfigPostExec (String ConfigPostExec) { this.ConfigPostExec = ConfigPostExec ; }
    public String getConfigPostExec() { return this.ConfigPostExec; }

    public void setMessageXSD (String MessageXSD) { this.MessageXSD = MessageXSD ; }
    public String getMessageXSD() { return this.MessageXSD; }

    public void setMessageXSLT (String MessageXSLT) { this.MessageXSLT = MessageXSLT ; }
    public String getMessageXSLT () { return this.MessageXSLT; }

    public void setEnvelopeXSLTExt (String EnvelopeXSLTExt) { this.EnvelopeXSLTExt = EnvelopeXSLTExt ; }
    public String getEnvelopeXSLTExt () { return this.EnvelopeXSLTExt; }
    public void setEnvelopeXSLTPost (String EnvelopeXSLTPost) { this.EnvelopeXSLTPost = EnvelopeXSLTPost ; }
    public String getEnvelopeXSLTPost () { return this.EnvelopeXSLTPost; }

    public void setEnvelopeNS (String EnvelopeNS) { this.EnvelopeNS = EnvelopeNS ; }
    public String getEnvelopeNS () { return this.EnvelopeNS ; }

    //public void setMessageAck (String MessageAck) { this.MessageAck = MessageAck ; }

    public void setMsgAnswXSLT (String MsgAnswXSLT) { this.MsgAnswXSLT = MsgAnswXSLT ; }
    public String getMsgAnswXSLT () { return this.MsgAnswXSLT; }

    //public void setMessageAnswAck (String MessageAnswAck) { this.MessageAnswAck = MessageAnswAck ; }
    //public void setMessageAnswerXSD (String MessageAnswerXSD) { this.MessageAnswerXSD = MessageAnswerXSD ; }
    //public void setMessageAnswMsgXSLT (String MessageAnswMsgXSLT) { this.MessageAnswMsgXSLT = MessageAnswMsgXSLT ; }
    //public void setMessageAnswHdXSLT (String MessageAnswHdXSLT) { this.MessageAnswHdXSLT = MessageAnswHdXSLT ; }
    //public void setAckXSD (String AckXSD) { this.AckXSD =  AckXSD; }

    public void setAckXSLT (String AckXSLT) { this.AckXSLT = AckXSLT ; }
    public String getAckXSLT () { return this.AckXSLT; }

    //public void setAnswAckXSLT (String AnswAckXSLT) { this.AnswAckXSLT = AnswAckXSLT ; }
    //public void setAnswAckHdXSLT (String AnswAckHdXSLT) { this.AnswAckHdXSLT = AnswAckHdXSLT ; }
    public String getErrTransXSLT () { return this.ErrTransXSLT; }
    public void setErrTransXSLT (String ErrTransXSLT) { this.ErrTransXSLT =  ErrTransXSLT ; }
    //public void setErrTransXSD (String ErrTransXSD) { this.ErrTransXSD = ErrTransXSD ; }
    //public void setHeaderXSD (String ErrTransXSD) { this.ErrTransXSD = ErrTransXSD ; }

    public void setHeaderXSLT (String HeaderXSLT) { this.HeaderXSLT = HeaderXSLT ; }
    public String getHeaderXSLT () { return this.HeaderXSLT; }


}
