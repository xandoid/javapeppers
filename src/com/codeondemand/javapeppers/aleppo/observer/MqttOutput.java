package com.codeondemand.javapeppers.aleppo.observer;

import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;

import java.util.Observable;

/**
 * This class writes to an MQTT compliant broker. The broker infomation needs to
 * be set in the configuration file.
 *
 * @author gfa
 */
public class MqttOutput extends NullObserver {

    public boolean close() {
        boolean retval = false;
        // TODO Auto-generated method stub
        try {
            if (mqtt_pub != null) {
                mqtt_pub.disconnect();
                retval = true;
            }
        } catch (MqttPersistenceException e) {
            logger.error(e.toString());
        } catch (MqttException e) {
            logger.error(e.toString());
        }
        return retval;
    }

    public void activate() {
        if (!initialized) {
            initialized = doInitialization();
        }
        if (pmap.containsKey("initmsg")) {
            try {
                mqtt_pub.publish(topics[0], ((String) pmap.get("initmsg")).getBytes(), bqos[0], false);
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public void update(Observable f, Object data) {
        if (!initialized) {
            initialized = doInitialization();
        }

        // System.out.println(data.getClass().getCanonicalName());
        if (data instanceof String && data.toString().trim().length() > 0) {
            byte[] payload = null;
            try {
                payload = data.toString().getBytes();
                // System.out.println(new String(">>>> " + data.toString()));
                mqtt_pub.publish(topics[0], payload, bqos[0], false);
            } catch (MqttException ex) {
                // ex.printStackTrace();
                // Client may have become disconnected.
                logger.error("Trying to reconnect client");
                if (!mqtt_pub.isConnected()) {
                    initialized = doInitialization();
                    try {
                        mqtt_pub.publish(topics[0], payload, bqos[0], false);
                    } catch (MqttException e) {
                        // TODO Auto-generated catch block
                        logger.error(e.toString());
                    }
                }
            }
        } else {
            System.out.println(data.getClass());
            System.out.println(data.toString());
        }
    }

    public boolean doInitialization() {
        boolean retval = false;

        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);

        String client = null;
        topics[0] = "test";
        String mqtt_host = "localhost";
        int mqtt_port = 1883;

        if (pmap.containsKey("mqtt.host")) {
            mqtt_host = pmap.get("mqtt.host").toString();
        }
        if (pmap.containsKey("mqtt.qos")) {
            String temp = pmap.get("mqtt.qos").toString().trim();
            if (temp.equals("1")) {
                bqos[0] = (byte) 1;
            }
            if (temp.equals("2")) {
                bqos[0] = (byte) 2;
            }
        }
        if (pmap.containsKey("mqtt.port")) {
            mqtt_port = Integer.parseInt((String) pmap.get("mqtt.port"));
        }
        if (pmap.containsKey("mqtt.topic")) {
            topics[0] = (String) pmap.get("mqtt.topic");
        }
        if (pmap.containsKey("mqtt.client")) {
            client = (String) pmap.get("mqtt.client");
        }
        if (pmap.containsKey("mqtt.uid")) {
            String uid = (String) pmap.get("mqtt.uid");
            opts.setUserName(uid);
        }
        if (pmap.containsKey("mqtt.pwd")) {
            String pwd = (String) pmap.get("mqtt.pwd");
            if (pmap.containsKey("mqtt.pwd_encrypted")) {
                pwd = MiscUtil.decodeB64String(pwd);
            }
            opts.setPassword(pwd.toCharArray());
        }
        String cstring = "tcp://" + mqtt_host + ":" + mqtt_port;

        // System.out.println(client);
        try {
            mqtt_pub = new org.eclipse.paho.client.mqttv3.MqttClient(cstring, client);

            mqtt_pub.connect(opts);
            retval = true;

        } catch (MqttException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return retval;
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttOutput");

    protected MqttClient mqtt_pub = null;
    protected byte[] bqos = new byte[1];
    protected String[] topics = new String[1];
    protected boolean initialized = false;

}
