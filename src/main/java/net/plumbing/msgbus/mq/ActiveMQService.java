package net.plumbing.msgbus.mq;

import jakarta.jms.*;

import net.plumbing.msgbus.common.sStackTrace;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;


import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.util.Arrays;
// @Configuration
//

public class ActiveMQService {
    protected BrokerService MQbroker=null;
    protected ActiveMQConnectionFactory MQconnectionFactory=null;
    protected TopicConnection JMSTopicConnection=null;
    protected Connection JMSQueueConnection=null;

    public static final String DESTINATION_NAME = "Q.Sender.OUT";
    public static final String SenderDESTINATION_NAME= "Q.Sender.IN";
    public static final Logger ActiveMQService_Log = LoggerFactory.getLogger(ActiveMQService.class);

  //  @Bean
    public ActiveMQDestination FirstQueue( String pDESTINATION_NAME ) {
        return new ActiveMQQueue(pDESTINATION_NAME);
    }

 //   @Bean
    public ActiveMQDestination NewQueue(String pDESTINATION_NAME ) throws Exception {
        if ( MQbroker != null ) {
            ActiveMQDestination[] CurrentListActiveMQDestination = MQbroker.getDestinations();
            if ( CurrentListActiveMQDestination != null ) {
                ActiveMQDestination[] NewListActiveMQDestination = Arrays.copyOf(CurrentListActiveMQDestination, CurrentListActiveMQDestination.length + 1);
                ActiveMQQueue activeMQQueue = new ActiveMQQueue(pDESTINATION_NAME);
                NewListActiveMQDestination[CurrentListActiveMQDestination.length] = activeMQQueue;

                MQbroker.setDestinations(NewListActiveMQDestination);
                ActiveMQService_Log.warn("ActiveMQbroker MsgBus: NewQueue=> '" + pDESTINATION_NAME + "' is set. "
                        + activeMQQueue.getQualifiedName() + " [" + activeMQQueue.getQueueName()
                        + "] Total MQDestination = " + NewListActiveMQDestination.length);
                return NewListActiveMQDestination[CurrentListActiveMQDestination.length];
            }
            else {
                //ActiveMQQueue activeMQQueue = new ActiveMQQueue(pDESTINATION_NAME);
                ActiveMQDestination MQDestination =  FirstQueue(pDESTINATION_NAME );
                MQbroker.setDestinations(new ActiveMQDestination[]{ MQDestination });

                return MQDestination;
            }

        }
        ActiveMQService_Log.error("ActiveMQbroker MsgBus is not init!" );
        return null;
    }

    @Bean
    public void deleteAllMessages() {
        try {
            if ( MQbroker != null ) MQbroker.deleteAllMessages();
        }
        catch (java.io.IOException e) {
            sStackTrace.strInterruptedException(e);
        }
    }

   public  Destination getDestination( String pDESTINATION_NAME) throws Exception {
       ActiveMQDestination[] CurrentListActiveMQDestination = MQbroker.getDestinations();
       ActiveMQDestination MQDestination;
       int i;
       for ( i=0; i < CurrentListActiveMQDestination.length; i ++ )
       {
           MQDestination = CurrentListActiveMQDestination[i];
           if ( MQDestination.getQualifiedName().indexOf( pDESTINATION_NAME) > 0 )
           {return  null;
               //Destination JMSdestination =  MQbroker.getDestination(MQDestination);
           }
       }
       return  null;
   }
   public Connection  StartMQbroker() throws Exception {
       MQbroker.start();
       File MQbrokerFile =  MQbroker.getDataDirectoryFile();
       ActiveMQService_Log.info( "MQbrokerFile:" + MQbrokerFile.getPath() + "/" + MQbrokerFile.getName()  );
       Connection Qconnection = MQconnectionFactory.createConnection();
       Qconnection.setClientID("JMS.Sender.QUEUEz");
       Qconnection.start();
       Session Qsession = Qconnection.createSession(false,
               Session.AUTO_ACKNOWLEDGE);
       Destination JMSdestination = Qsession.createQueue(SenderDESTINATION_NAME);
       MessageProducer producer = Qsession.createProducer(JMSdestination);
       // We will send a small text message saying 'Hello World!!!'
       TextMessage message = Qsession.createTextMessage("{Hello !!! Welcome to the world of ActiveMQ.}");
       producer.send(message);
       message = Qsession.createTextMessage("{ Hi !!! good HI  the world of ActiveMQ.}");
       producer.send(message);
       /*message = Qsession.createTextMessage("{ NO !!! dead  world of ActiveMQ.}");
       producer.send(message);
       message = Qsession.createTextMessage("{ By !!! good by  the world of ActiveMQ.}");
       producer.send(message);
        */
       /*
       TopicConnection topicConnection = null;
       topicConnection = MQconnectionFactory.createTopicConnection();
       topicConnection.setClientID("JMS.Sender.TOPIC");
       TopicSession topicConsumerSession = topicConnection.createTopicSession(
               false, Session.AUTO_ACKNOWLEDGE);
       Topic topic = topicConsumerSession.createTopic("Ack.Sender.Topic");
       topicConnection.start();

       // Publish
       TopicSession topicPublisherSession = topicConnection.createTopicSession(
               false, Session.AUTO_ACKNOWLEDGE);
       String payload = "Important Task";
       Message msg = topicPublisherSession.createTextMessage(payload);
       TopicPublisher publisher = topicPublisherSession.createPublisher(topic);
       ActiveMQService_Log.info("TopicPublisher Sending text '" + payload + "'");
       publisher.publish(msg);
       msg = topicPublisherSession.createTextMessage(payload);
       publisher.publish(msg);
*/
       //Creating a non transactional session to send/receive JMS message.

       /*Destination JMSdestination  = session.createTopic(DESTINATION_NAME); //.createQueue(DESTINATION_NAME);
       ActiveMQService_Log.info( "JMSdestination(" +  JMSdestination.toString() );
       // MessageConsumer is used for receiving (consuming) messages
       */
       // this.JMSTopicConnection = topicConnection;
       this.JMSQueueConnection = Qconnection;
       return Qconnection;
   }
    @Bean
    public BrokerService ActiveMQbroker( String bindAddressConnectMsgBus ) throws Exception {
        if ( MQbroker == null ) {
            final BrokerService broker = new BrokerService();
            // TransportConnector tcp_connection =broker.addConnector("tcp://localhost:61216");
            ActiveMQService_Log.info("ActiveMQbroker MsgBus init for :" + bindAddressConnectMsgBus );
            TransportConnector tcp_connection =broker.addConnector(bindAddressConnectMsgBus);
            TransportConnector vm_connection = broker.addConnector("vm://localhost:77077");
            broker.setPersistent(false);
            broker.setAllowTempAutoCreationOnSend(true);
            broker.setBrokerId("MsgBus.Sender.X");
            broker.setCacheTempDestinations(true);
            broker.setBrokerName("MsgBusX");
            broker.setStartAsync(false);
            //broker.setDestinations(new ActiveMQDestination[]{queue()});
            //broker.getDataDirectoryFile();
            MQbroker= broker;
            ActiveMQService_Log.info("ActiveMQbroker MsgBus preSet" );
            ActiveMQService_Log.info("ActiveMQbroker MsgBus VM connect: [" +  vm_connection.getConnectUri() +"]" ) ;
            ActiveMQService_Log.info("ActiveMQbroker MsgBus TCP connect: [" +  tcp_connection.getConnectUri() +"]") ;

            // Create a Connection
            MQconnectionFactory = new ActiveMQConnectionFactory(vm_connection.getConnectUri());

            return broker;
        }
        else return MQbroker;
    }
}
