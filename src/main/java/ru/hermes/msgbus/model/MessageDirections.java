package ru.hermes.msgbus.model;

import java.util.HashMap;
import java.util.Set;
//import org.springframework.context.annotation.Bean;

//import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
// import ru.hermes.msgbus.model.MessageDirectionsVO;

@Component
//@Scope("prototype")

public  class MessageDirections {
   // public static HashMap<Integer, MessageDirectionsVO > AllMessageDirections;
    public static int RowNum=0;
    public static HashMap<Integer, MessageDirectionsVO > AllMessageDirections = new HashMap<Integer, MessageDirectionsVO >();

    public static int sizeAllMessageDirections() {
        return AllMessageDirections.size();
    }
    public static Set<Integer> keysAllMessageDirections() {
        return AllMessageDirections.keySet();
    }

}
