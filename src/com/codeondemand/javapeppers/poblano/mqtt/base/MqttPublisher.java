// --------------------------------------------------------------------------
//  javapeppers Confidential
//  
// --------------------------------------------------------------------------
package com.codeondemand.javapeppers.poblano.mqtt.base;


import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;

/**
 * This is a simple publishing class for sending messages to the MQTT broker.
 *
 * @author gfa
 */
public class MqttPublisher {

    // ***********************************************************************
    // Constructors
    // ***********************************************************************

    /**
     * Constructor for MqttPublisher
     */
    public MqttPublisher() {
    }

    /**
     * Constructor for initializing the publisher.
     *
     * @param mqtt The MqttClient connected to the broker.
     */
    public MqttPublisher(MqttClient mqtt) {
        this.mqtt = mqtt;
    }

    // ***********************************************************************
    // Public methods and data
    // ***********************************************************************

    /**
     * A simple wrapper for the WMQTT publish method. If a problem is detected
     * then an exception is thrown and an error
     *
     * @param topics   The topic on which the data will be published.
     * @param message  The data to be published
     * @param qos      The Quality of Service at which the publication should be
     *                 delivered.
     * @param retained Is this a retained publication or not?
     */
    public void publish(String[] topics, String message, byte qos, boolean retained) throws Exception {
        try {
            if (message != null) {
                byte[] payload = message.toString().getBytes();
                mqtt.publish(topics[0], payload, qos, retained);
                //System.err.println( message);
            }
        } catch (MqttException ex) {
            logger.error(ex.toString());
            throw ex;
        }
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttPublisher");

    // ***********************************************************************
    // Protected methods and data
    // ***********************************************************************
    /**
     * The MqttClient
     */
    protected MqttClient mqtt = null;

    // ***********************************************************************
    // Private data and methods
    // ***********************************************************************

}
