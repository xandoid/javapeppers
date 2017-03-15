// --------------------------------------------------------------------------
//  javapeppers Confidential
//  
// --------------------------------------------------------------------------
package com.codeondemand.javapeppers.poblano.mqtt.base;

import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * A very simple publishing class that publishes a single message and quits. It
 * is useful when publishing back when a message is received from the broker
 * since that must be done on a new thread.
 *
 * @author gfa
 */
public class MqttPublisherThread extends MqttPublisher implements Runnable {

    // ***********************************************************************
    // Constructors
    // ***********************************************************************

    /**
     * Basic constructor for MqttFilePublisher
     */
    public MqttPublisherThread() {
    }

    /**
     * Constructor for initializing the publisher.
     *
     * @param mqtt    The MqttClient connected to the broker.
     * @param topic   The topic to publish on.
     * @param message The message to publish.
     * @param qos     The quality of service for publishing (0,1,2)
     */
    public MqttPublisherThread(MqttClient mqtt, String topic, String message, byte qos) {
        this.mqtt = mqtt;
        this.topic = new String(topic);

        this.qos = qos;
        if (message != null) {
            setMsg(message);
        }
    }

    // ***********************************************************************
    // Public methods and data
    // ***********************************************************************

    /**
     * Sets the message to be published.
     *
     * @param message The message to be published.
     */
    // public synchronized void setMsg(String message){
    public void setMsg(String message) {
        queue.add(message);
    }

    public synchronized void setFinished() {
        this.notFinished = false;
    }

    public void setSleepInterval(long l) {
        sleep_interval = l;
    }

    // ***********************************************************************
    // Implementation for Runnable interface
    // ***********************************************************************
    public void run() {
        while (notFinished || !queue.isEmpty()) {
            try {
                if (queue.isEmpty()) {
                    Thread.yield();
                } else {
                    doPublish(queue.poll());
                }
            } catch (Exception pex) {
                logger.error(pex.toString());
            }
        }
        try {
            mqtt.disconnect();
        } catch (MqttException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private synchronized void doPublish(String message) {
        try {
            String[] topics = new String[1];
            topics[0] = topic;

            if (message != null) {
                Thread.sleep(sleep_interval);
                //this.wait(sleep_interval);
                mqtt.publish(topic, message.getBytes(), qos, false);
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }
    // ***********************************************************************
    // Private data and methods
    // ***********************************************************************
    // private String  message = null;

    private String topic = null;
    private byte qos = 0;
    private boolean notFinished = true;
    private long sleep_interval = 0L;
    protected ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttPublisherThread");


}

