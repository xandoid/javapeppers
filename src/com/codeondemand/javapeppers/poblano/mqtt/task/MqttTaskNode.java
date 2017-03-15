//--------------------------------------------------------------------------
//javapeppers Confidential
//
//--------------------------------------------------------------------------
package com.codeondemand.javapeppers.poblano.mqtt.task;

import com.codeondemand.javapeppers.poblano.mqtt.base.MqttBrokerInfo;
import com.codeondemand.javapeppers.poblano.mqtt.base.MqttConnector;
import com.codeondemand.javapeppers.poblano.mqtt.base.MqttSubscriber;
import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.Hashtable;

public abstract class MqttTaskNode extends MqttSubscriber implements MqttCallback {

    public MqttTaskNode() {
        this.name = new String(new Integer(this.hashCode()).toString());
    }

    public MqttTaskNode(String name) {
        this.name = new String(name);
    }

    public void setTaskName(String name) {
        this.name = new String(name);
    }

    public void setPubName(String name) {
        this.pubName = new String(name);
    }

    public void setSubName(String name) {
        this.subName = new String(name);
    }

    public void setPubBroker(MqttBrokerInfo broker) {
        this.pubBroker = broker;
    }

    public void setSubBroker(MqttBrokerInfo broker) {
        this.subBroker = broker;
    }

    public void initialize() {
        connector = new MqttConnector();
        if (pubBroker != null) {
            if (pubName == null) {
                pubName = new String(name);
            }
            mqtt_pub = connector.addConnection(name, pubName, pubBroker);
            System.out.println(name + "mqtt_pub" + mqtt_pub);
        }
        if (subBroker != null) {
            if (subName == null) {
                subName = new String(name);
            }
            mqtt_sub = connector.addConnection(name, subName, subBroker);
            System.out.println(name + "mqtt_sub" + mqtt_sub);
            if (sub_topic != null) {
                this.subscribe(sub_topic, qos);
            }
        }

        register();
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttTaskNode");

    public abstract void register();

    protected void disconnect() {
        System.out.println("Disconnecting:" + this.getClass().toString());
        try {
            if (mqtt_pub != null) {
                mqtt_pub.disconnect();
            }
            if (mqtt_sub != null) {
                mqtt_sub.disconnect();
            }
        } catch (MqttException mqtte) {
            logger.error(mqtte.toString());
        }
    }

    public abstract void messageArrived(String topic, MqttMessage msg);

    public void connectionLost(Throwable t) {
        //TO-DO: Handle this.
    }

    private String name = null;
    private String pubName = null;
    private String subName = null;
    private MqttClient mqtt_pub = null;
    private MqttBrokerInfo pubBroker = null;
    private MqttBrokerInfo subBroker = null;
    private String pub_topic = null;
    private String sub_topic = null;
    private MqttConnector connector = null;
    private byte qos = 0;

    protected Hashtable<String, String> fields = null;

    public void setFields(Hashtable<String, String> table) {
        fields = table;
    }

    public String getPub_topic() {
        return pub_topic;
    }

    public void setPub_topic(String pub_topic) {
        this.pub_topic = pub_topic;
    }

    public String getSub_topic() {
        return sub_topic;
    }

    public void setSub_topic(String sub_topic) {
        this.sub_topic = sub_topic;
    }

    public byte getQos() {
        return qos;
    }

    public void setQos(byte qos) {
        this.qos = qos;
    }

    public MqttClient getPub_client() {
        return mqtt_pub;
    }
}
