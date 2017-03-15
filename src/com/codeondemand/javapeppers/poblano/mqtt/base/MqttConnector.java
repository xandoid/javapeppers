// --------------------------------------------------------------------------
//  javapeppers Confidential
//  
// --------------------------------------------------------------------------

package com.codeondemand.javapeppers.poblano.mqtt.base;

import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.util.Hashtable;

/**
 * This manages connections to multiple brokers for an MQTT based component or
 * application.
 *
 * @author gfa
 */
public class MqttConnector {

    // ***********************************************************************
    // Constructors
    // ***********************************************************************

    /**
     * Constructor for MqttConnection
     */
    public MqttConnector() {
    }

    // ***********************************************************************
    // Public methods and data
    // ***********************************************************************

    /**
     * This initialization function provides the functionality to control all
     * the aspects of the connection including the retry interval and whether or
     * not a clean start is made for this connection.
     */
    public MqttClient addConnection(String name, String client, MqttBrokerInfo broker, boolean cleanstart) {
        this.cleanStart = cleanstart;
        return addConnection(name, client, broker);

    }

    /**
     * Connects to the MQTT based using a TCP type of connection.
     *
     * @param name   The name for the connection.
     * @param client A unique string to use when connection to the broker.
     * @param broker The MqttBrokerInfo object
     * @return An MqttClient object with a connection to the broker.
     */
    public MqttClient addConnection(String name, String client, MqttBrokerInfo broker) {

        mqtt = null;
        if (broker != null) {
            logger.debug("Adding connection: " + name + ":" + client + ":" + broker.getName());

            if (connections.get(name + "_" + broker.getName()) != null) {
                logger.debug("Connection already exists");
                return connections.get(name + "_" + broker.getName());
            }

            String ip = broker.getIp();
            String port = broker.getPort();
            String connStr = "tcp://" + ip + ":" + port;

            logger.debug(connStr.toString());

            try {
                MqttConnectOptions opts = new MqttConnectOptions();

                if (mqtt == null) {
                    //MemoryPersistence persistence = null;
                    opts.setCleanSession(cleanStart);
                    mqtt = new MqttClient(connStr, client);
                }
                mqtt.connect(opts);

                connections.put(name + "_" + broker.getName(), mqtt);
            } catch (MqttException mqe) {
                logger.error("MQTT connect failed for client: " + client);
                logger.error(mqe.toString());
            } catch (Exception ex) {
                logger.error("MQTT connect failed for client: " + client);
                logger.error(ex.toString());
            }
        }
        return mqtt;
    }

    /**
     * Retrieves the MqttClient associated with a specified named connection
     *
     * @param name The name of the connection that should be retrieved.
     * @return Returns null if there is no such named connnection
     */
    public MqttClient getConnection(String name) {
        return connections.get(name) != null ? connections.get(name) : null;
    }

    /**
     * A wrapper for the WMQTT disconnect method
     */
    public void deleteConnection(String name) {

        if (connections.get(name) != null) {
            try {
                connections.get(name).disconnect();
                connections.remove(name);
            } catch (Exception ex) {
                logger.error(ex.toString());
            }
        }
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttConnector");

    // ***********************************************************************
    // Protected methods and data
    // ***********************************************************************

    // ***********************************************************************
    // Private data and methods
    // ***********************************************************************
    private Hashtable<String, MqttClient> connections = new Hashtable<String, MqttClient>();
    private MqttClient mqtt = null;
    private boolean cleanStart = true;

}
