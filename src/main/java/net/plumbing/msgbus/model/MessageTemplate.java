package net.plumbing.msgbus.model;

import java.util.HashMap;

import org.springframework.stereotype.Component;
// import MessageDirectionsVO;

@Component
//@Scope("prototype")

public class MessageTemplate {
    public static int RowNum=0;
    public static HashMap<Integer, MessageTemplateVO > AllMessageTemplate = new HashMap<Integer, MessageTemplateVO >();
    //public static ArrayList AllMessageTemplate = new ArrayList();
}
