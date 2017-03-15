package com.codeondemand.javapeppers.poblano.mqtt.task;

import java.util.Hashtable;

public class MqttTaskInfo {

    public MqttTaskInfo(String name) {
        this.taskname = name;
    }

    private String taskname = null;
    private String pub_broker = null;
    private String sub_broker = null;
    private String pub_topic = null;
    private String sub_topic = null;
    private String qos = null;
    private String classname = null;
    private Hashtable<String, String> fields = new Hashtable<String, String>();

    public void setField(String field, String value) {
        fields.put(field, value);
    }

    public String getField(String key) {
        return fields.get(key);
    }

    public String getTaskname() {
        return taskname;
    }

    public String getPub_broker() {
        return pub_broker;
    }

    public void setPub_broker(String pub_broker) {
        this.pub_broker = pub_broker;
    }

    public String getSub_broker() {
        return sub_broker;
    }

    public void setSub_broker(String sub_broker) {
        this.sub_broker = sub_broker;
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

    public String getQos() {
        return qos;
    }

    public void setQos(String qos) {
        this.qos = qos;
    }

    public String getClassname() {
        return classname;
    }

    public void setClassname(String classname) {
        this.classname = classname;
    }

    public Hashtable<String, String> getFields() {
        return fields;
    }
}
