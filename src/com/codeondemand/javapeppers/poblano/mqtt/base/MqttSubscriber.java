// --------------------------------------------------------------------------
//  
// --------------------------------------------------------------------------

package com.codeondemand.javapeppers.poblano.mqtt.base;


import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * This is an abstract class that provides basic subscription service to
 * the MQTT broker.
 *
 * @author gfa
 */
public  class MqttSubscriber implements MqttCallback{

    //***********************************************************************
    // Constructors
    //***********************************************************************
    /**
     * Basic constructor for MqttSubscriber
     */
    public MqttSubscriber() {
    }

    //***********************************************************************
    // Public methods and data
    //***********************************************************************
	
	public void connectionLost(Throwable arg0) {
		// TODO Auto-generated method stub
		
	}

	
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub
		
	}


	public void messageArrived(String topic, MqttMessage msg) throws Exception {
		byte[] payload = msg.getPayload();
		System.out.println( new String(payload));
	}
    /**
     * Requests a subscription to the message broker .  You must have
     * passed a MqttClient object in one of the constructors for this
     * to work.
     *
     * @param topic The topic that should be subscribed.
     * @param qos   The quality of service for the subscription (0,1,2)
     */
    protected void subscribe( String topic, byte qos ){
        try {
            String[] theseTopics = new String[1];
            byte[] theseQoS = new byte[1];

            theseTopics[0] = topic;
            theseQoS[0] = qos;
            mqtt_sub.subscribe( theseTopics);

        } catch ( MqttException ex ) {
            ex.printStackTrace();
            logger.error(ex.toString());
        }
    }

    /**
     * Requests a subscription be terminated.
     *
     * @param topic The topic that should be unsubscribed.
     */
    public void unsubscibe( String topic){

        try {
            String[] theseTopics = new String[1];

            theseTopics[0] = topic;
            mqtt_sub.unsubscribe( theseTopics);

        } catch ( MqttException ex ) {
            ex.printStackTrace();
            logger.error(ex.toString());
        }
    }
    
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttSubscriber");

    //***********************************************************************
    // Protected methods and data
    //***********************************************************************

    /** The MqttClient object that is connected to the broker. */
    protected MqttClient  mqtt_sub         = null;

    //***********************************************************************
    // Private data and methods
    //***********************************************************************
}

