package ru.hermes.msgbus.model;

public class MessageDirectionsVO {
/*
*/
    private int MsgDirection_Id;
    private String MsgDirection_Cod;
    private String MsgDirection_Desc;
    private String App_Server;
    private String WSDL_Name;
    private String MsgDir_own;
    private String Operator_Id;
    private int Type_Connect;
    private String Db_Name;
    private String Db_user;
    private String Db_pswd;
    private String Subsys_Cod;
    private int Base_Thread_Id;
    private int Num_Thread;
    private int Short_retry_count;
    private int Short_retry_interval;
    private int Long_retry_count;
    private int Long_retry_interval;
    private int Num_Helpers_Thread;
    private String List_Lame_Threads;


    public void setMessageDirectionsVO( int MsgDirection_Id,
             String MsgDirection_Cod,
             String MsgDirection_Desc,
             String App_Server,
             String WSDL_Name,
             String MsgDir_own,
             String Operator_Id,
             int Type_Connect,
             String Db_Name,
             String Db_user,
             String Db_pswd,
             String Subsys_Cod,
             int Base_Thread_Id,
             int Num_Thread,
             int Short_retry_count,
             int Short_retry_interval,
             int Long_retry_count,
             int Long_retry_interval,
             int Num_Helpers_Thread, String List_Lame_Threads) {
         this.MsgDirection_Id = MsgDirection_Id;
         this.MsgDirection_Cod = MsgDirection_Cod;
        this.MsgDirection_Desc = MsgDirection_Desc;
         this.App_Server = App_Server;
        this.WSDL_Name = WSDL_Name;
         this.MsgDir_own = MsgDir_own;
         this.Operator_Id = Operator_Id;
         this.Type_Connect = Type_Connect;
        this.Db_Name = Db_Name;
        this.Db_user = Db_user;
         this.Db_pswd = Db_pswd;
          this.Subsys_Cod = Subsys_Cod;
         this.Base_Thread_Id = Base_Thread_Id;
        this.Num_Thread = Num_Thread;
        this.Short_retry_count = Short_retry_count;
        this.Short_retry_interval = Short_retry_interval;
        this.Long_retry_count = Long_retry_count;
        this.Long_retry_interval = Long_retry_interval;
        this.Num_Helpers_Thread = Num_Helpers_Thread;
        this.List_Lame_Threads = List_Lame_Threads;
    }
    public String getList_Lame_Threads() { return this.List_Lame_Threads; }
    public int getNum_Helpers_Thread(){ return this.Num_Helpers_Thread;    }
    public int getMsgDirection_Id(){ return this.MsgDirection_Id;    }
    public String getMsgDirection_Cod() { return this.MsgDirection_Cod;   }
    public String getMsgDirection_Desc(){ return this.MsgDirection_Desc;    }
    public String getApp_Server(){ return this.App_Server;    }
    public String getWSDL_Name(){ return this.WSDL_Name;    }
    public String getMsgDir_own(){ return this.MsgDir_own;    }
    public String getOperator_Id(){ return this.Operator_Id;    }
    public int getType_Connect(){ return this.Type_Connect;    }
    public String getDb_Name(){ return this.Db_Name;    }
    public String getDb_user(){ return this.Db_user;    }
    public String getDb_pswd(){ return this.Db_pswd;    }
    public String getSubsys_Cod(){ return this.Subsys_Cod;    }
    public int getBase_Thread_Id(){ return this.Base_Thread_Id;    }
    public int getNum_Thread(){ return this.Num_Thread;    }

    public int getShort_retry_count(){ return        this.Short_retry_count ; }
    public int getShort_retry_interval(){ return    this.Short_retry_interval ; }
    public int getLong_retry_count(){ return   this.Long_retry_count ; }
    public int getLong_retry_interval(){ return    this.Long_retry_interval ; }

    public void setShort_retry_count(int Short_retry_count){ this.Short_retry_count=Short_retry_count ; }
    public void setShort_retry_interval(int Short_retry_interval){ this.Short_retry_interval =Short_retry_interval; }
    public void setLong_retry_count(int Long_retry_count){ this.Long_retry_count = Long_retry_count; }
    public void setLong_retry_interval(int Long_retry_interval){ this.Long_retry_interval = Long_retry_interval; }
/********************************************************************************************************************
    public  void setMsgDirection_Id(int MsgDirection_Id){  this.MsgDirection_Id = MsgDirection_Id;    };
    public  void setMsgDirection_Cod() {  this.MsgDirection_Cod = MsgDirection_Cod;   };
    public  void setMsgDirection_Desc(String MsgDirection_Desc){  this.MsgDirection_Desc = MsgDirection_Desc;    };
    public  void setApp_Server(String App_Server){  this.App_Server = App_Server;    };
    public  void setWSDL_Name(String WSDL_Name){  this.WSDL_Name = WSDL_Name;    };
    public  void setMsgDir_own(String MsgDir_own){  this.MsgDir_own = MsgDir_own;    };
    public  void setOperator_Id( String Operator_Id){  this.Operator_Id = Operator_Id;    };

    public  void setDb_Name(String Db_Name){  this.Db_Name = Db_Name;    };
 */
    public  void setWSDL_Name(String WSDL_Name){  this.WSDL_Name = WSDL_Name;    };
    public  void setType_Connect(int Type_Connect){  this.Type_Connect = Type_Connect;    };
    public  void setDb_user(String Db_user){  this.Db_user = Db_user;    };
    public  void setDb_pswd(String Db_pswd){  this.Db_pswd = Db_pswd;    };
    public  void setBase_Thread_Id(int Base_Thread_Id){  this.Base_Thread_Id = Base_Thread_Id;    };
    public  void setNum_Thread(int Num_Thread){  this.Num_Thread = Num_Thread;    };
/*
    public  void setSubsys_Cod(String Subsys_Cod){  this.Subsys_Cod = Subsys_Cod;    };
    public  void setBase_Thread_Id(int Base_Thread_Id){  this.Base_Thread_Id = Base_Thread_Id;    };
    public  void setNum_Thread(int Num_Thread){  this.Num_Thread = Num_Thread;    };
********************************************************************************************************************/
    public String LogMessageDirections( ) {
        return ( //"" + nKey +
                        " f.msgdirection_id = " + MsgDirection_Id +
                        ", f.msgdirection_cod=" + MsgDirection_Cod +
                        ", f.msgdirection_desc= " + MsgDirection_Desc +
                        //", f.app_server= " + App_Server +
                        ", f.wsdl_name= " + WSDL_Name +
                        //", f.msgdir_own= " + MsgDir_own +
                        //", f.operator_id= " + Operator_Id +
                        ", f.type_connect= " + Type_Connect +
                        //", f.db_name= " + Db_Name +
                        ", f.db_user= " + Db_user +
                        ", f.db_pswd= " + Db_pswd +
                        ", f.subsys_cod= " + Subsys_Cod +
                        ", f.base_thread_id= " + Base_Thread_Id +
                        ", f.num_thread = " + Num_Thread +
                        ", f.Num_Helpers_Thread = " + Num_Helpers_Thread
                );
    }
}
