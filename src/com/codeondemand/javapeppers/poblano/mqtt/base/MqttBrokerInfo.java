// --------------------------------------------------------------------------
//   Confidential
//  
// --------------------------------------------------------------------------

package com.codeondemand.javapeppers.poblano.mqtt.base;

/**
 * A simple class that contains the name, ip, and port for a MQTT based
 * broker so that it can be identified by an application.
 *
 * @author gfa
 */
public class MqttBrokerInfo {

    public MqttBrokerInfo(String name, String ip, String port) {
        this.name = name;
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    private String ip = null;
    private String port = null;
    private String name = null;
    private String uid = null;
    private String pwd = null;

}
