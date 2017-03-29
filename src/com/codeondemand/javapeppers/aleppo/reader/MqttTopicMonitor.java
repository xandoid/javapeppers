package com.codeondemand.javapeppers.aleppo.reader;

import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.StringTokenizer;

/**
 * This class reads from an MQTT type broker and passes the message in a
 * DataCapsule named 'data' (this should be parameterized).
 *
 * @author gfa
 */
public class MqttTopicMonitor extends MqttReader implements MqttCallback {

    /**
     * Requests a subscription to the message broker . You must have passed a
     * MqttClient object in one of the constructors for this to work.
     *
     * @param topic The topic that should be subscribed.
     * @param qos   The quality of service for the subscription (0,1,2)
     */
    protected void subscribe(String topic, byte qos) {

        try {
            StringTokenizer stok = new StringTokenizer(topic, "|");
            if (stok.countTokens() > 0) {
                //while (stok.hasMoreTokens()){
                int n = stok.countTokens();
                String[] theseTopics = new String[n];
                byte[] theseQoS = new byte[n];
                for (int j = 0; j < n; j++) {
                    theseTopics[j] = stok.nextToken();
                    //System.out.println( theseTopics[j]);
                    theseQoS[j] = qos;
                }
                mqtt_sub.subscribe(theseTopics);
                //}
            } else {
                String[] theseTopics = new String[1];
                byte[] theseQoS = new byte[1];

                theseTopics[0] = topic;
                theseQoS[0] = qos;
                mqtt_sub.subscribe(theseTopics);

            }

        } catch (MqttException ex) {
            ex.printStackTrace();
            logger.error(ex.toString());
        }
    }

    private boolean omit_topic = false;
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttTopicMonitor");

    // ***********************************************************************
    // Public methods and data
    // ***********************************************************************
    public boolean doInitialization() {
        boolean retval = false;
        if (pmap.containsKey("omit_topic")) {
            omit_topic = Boolean.parseBoolean((String) pmap.get("omit_topic"));
        }
        retval = super.doInitialization();
        return retval;
    }

    @Override
    public void messageArrived(String topic, MqttMessage m) throws Exception {
        byte[] msg = m.getPayload();
        message = new String(msg);
        //System.out.println( topic+":"+message);
        if (input.size() < maxQueueBuffer) {
            if (omit_topic) {
                input.add((String) message);
            } else {
                input.add(topic + "|" + message);
            }
        } else {
            System.err.println("Max queue exceeded.");
        }
    }

}
