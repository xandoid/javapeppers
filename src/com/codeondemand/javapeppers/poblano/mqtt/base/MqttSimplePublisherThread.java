// --------------------------------------------------------------------------
//  javapeppers Confidential
//  
// --------------------------------------------------------------------------
package com.codeondemand.javapeppers.poblano.mqtt.base;


import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttClient;


/**
 * A very simple publishing class that publishes a single message and quits. It
 * is useful when publishing back when a message is received from the broker
 * since that must be done on a new thread.
 * 
 * @author gfa
 * 
 */
public class MqttSimplePublisherThread
             extends MqttPublisher
             implements Runnable{

    // ***********************************************************************
    // Constructors
    // ***********************************************************************

    /**
	 * Basic constructor for MqttFilePublisher
	 */
    public MqttSimplePublisherThread( ) {
    }

    /**
	 * Constructor for initializing the publisher.
	 * 
	 * @param mqtt
	 *            The MqttClient connected to the broker.
	 * @param topic
	 *            The topic to publish on.
	 * @param message
	 *            The message to publish.
	 * @param qos
	 *            The quality of service for publishing (0,1,2)
	 */
    public MqttSimplePublisherThread( MqttClient mqtt, String topic,
                                String message, byte qos  ){
        this.mqtt    = mqtt;
        this.topic   = new String(topic);
        this.mqtt    = mqtt;
        this.qos     = qos;       
        this.message = new String(message);
    }

    // ***********************************************************************
    // Public methods and data
    // ***********************************************************************

 

    // ***********************************************************************
    // Implementation for Runnable interface
    // ***********************************************************************
    public void run(){
    	try{
		    String[] topics = new String[1];
		    topics[0] = topic;
		    publish( topics ,message, qos, false );
    	}catch( Exception e){
			logger.error(e.toString());
		}
    }

    // ***********************************************************************
    // Private data and methods
    // ***********************************************************************
    private String  message = null;   
    private String  topic   = null;
    private byte    qos     = 0;

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttSimplePublisherThread");

}

