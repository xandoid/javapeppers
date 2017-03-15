/**
 *
 */
package com.codeondemand.javapeppers.aleppo.process;

import com.codeondemand.javapeppers.aleppo.connector.RecordConnector;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import com.codeondemand.javapeppers.poblano.mqtt.base.MqttSubscriber;
import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.*;

import java.util.ArrayList;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;

/**
 * ProcessLauncher is the the process control hub for Aleppo dataflow processes,
 * although it really just invokes an instance of ConfigurationLoader and then
 * runs each of the RecordConnectors in as separate thread in parallel. Since
 * each of these threads is running under the same JVM, you need to be aware of
 * memory issues.
 * <p>
 * Note: Since each of the connector blocks parsed by the ConfigurationLoader is
 * run in parallel, you need to be aware of any processing dependencies between
 * them. Typically you will only run totally independent processes in parallel.
 *
 * @author gfa
 */
public class MQTTProcessLauncher extends MqttSubscriber implements Observer, MqttCallback {

    public boolean process(ArrayList<RecordConnector> c) {

        // Allocate space for enough threads to run
        // all of the connectors.
        Thread[] threads = new Thread[c.size()];
        for (int i = 0; i < c.size(); i++) {
            RecordConnector r = c.get(i);
            r.addObserver(this);
            Thread t = new Thread(r);
            threads[i] = t;
            logger.debug("Starting thread for connector: " + t);
            t.start();
        }

        // Join each of the threads so that this thread will
        // wait for all of the processes to finish before
        // it completes. It is unimportant in which order
        // the Threads finish, since this main thread will
        // not exit until each thread in the collection is
        // finished.
        try {
            for (int i = 0; i < c.size(); i++) {
                if (threads[i].isAlive()) {
                    threads[i].join();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            success = false;
        }
        return success;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {

        Properties props = MiscUtil.loadXMLPropertiesFile(args[0]);

        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        opts.setUserName(props.getProperty("mqtt.uid"));
        opts.setPassword(props.getProperty("mqtt.pwd").toCharArray());

        String[] topics = new String[1];
        topics[0] = props.getProperty("mqtt.topic");
        String client = props.getProperty("mqtt.client");
        String mqtt_host = props.getProperty("mqtt.host");
        int mqtt_port = Integer.parseInt(props.getProperty("mqtt.port"));
        byte[] bqos = new byte[1];
        bqos[0] = Byte.parseByte(props.getProperty("mqtt.qos"));

        String cstring = "tcp://" + mqtt_host + ":" + mqtt_port;
        MQTTProcessLauncher t = new MQTTProcessLauncher();

        try {
            ;
            t.mqtt_sub = new MqttClient(cstring, client);
            t.mqtt_sub.setCallback(t);
            t.mqtt_sub.connect(opts);
            t.mqtt_sub.subscribe(topics);
            t.setLauncher(t);
        } catch (MqttException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void setLauncher(MQTTProcessLauncher p) {
        bar = p;
    }

    public void update(Observable o, Object arg) {
        if (o instanceof RecordConnector && arg instanceof Long) {
            // If the return value is negative, that indicates that
            // less than the minimum required records were moved.
            if ((Long) arg < 0L) {
                success = false;
            }
            logger.debug("Connector reports delivering " + (Long) arg + " record.");
        }
    }

    private boolean success = true;
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MQTTProcessLauncher");


    private MQTTProcessLauncher bar = null;

    /*
     * (non-Javadoc)
     *
     * @see com.codeondemand.javapeppers.mqttclient.MqttCallback#connectionLost(java.lang.Throwable)
     */
    public void connectionLost(Throwable arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
        ConfigurationLoader loader = new ConfigurationLoader();
        System.err.println(new String(arg1.getPayload()));
        ArrayList<RecordConnector> foo = loader.initializeWithString(new String(arg1.getPayload()));
        bar.process(foo);
    }
}
