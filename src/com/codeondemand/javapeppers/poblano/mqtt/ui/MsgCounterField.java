package com.codeondemand.javapeppers.poblano.mqtt.ui;

import com.codeondemand.javapeppers.poblano.mqtt.base.MqttBrokerInfo;
import com.codeondemand.javapeppers.poblano.mqtt.base.MqttConnector;
import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.*;

import javax.swing.*;
import java.awt.*;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class MsgCounterField extends JPanel implements MqttCallback {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        MsgCounterField foo = new MsgCounterField();
        MqttBrokerInfo bi = new MqttBrokerInfo("test", "127.0.0.1", "1883");

        MqttConnector c = new MqttConnector();
        MqttClient mqtt = c.addConnection("counter", "msgcounter", bi);
        foo.initialize(0, mqtt, "test");
        JFrame bar = new JFrame("counter");
        bar.setSize(500, 80);
        Container content = bar.getContentPane();

        FlowLayout flo = new FlowLayout();
        JPanel stuff = new JPanel(flo);

        GridLayout mlo = new GridLayout(1, 4);
        JPanel labelPanel = new JPanel(mlo);
        JLabel topicLabel = new JLabel();
        topicLabel.setAlignmentX(java.awt.Component.LEFT_ALIGNMENT);
        JLabel msgCountLabel = new JLabel();
        JLabel startTimeLabel = new JLabel();
        JLabel elapsedTimeLabel = new JLabel();

        FontMetrics fm = topicLabel.getFontMetrics(topicLabel.getFont());
        topicLabel.setPreferredSize(new Dimension(12 * fm.charWidth('A'), fm.getHeight()));
        msgCountLabel.setPreferredSize(new Dimension(12 * fm.charWidth('A'), fm.getHeight()));
        startTimeLabel.setPreferredSize(new Dimension(12 * fm.charWidth('A'), fm.getHeight()));
        elapsedTimeLabel.setPreferredSize(new Dimension(12 * fm.charWidth('A'), fm.getHeight()));
        topicLabel.setText("Topic name");
        msgCountLabel.setText("msg count");
        startTimeLabel.setText("start time");
        elapsedTimeLabel.setText("elapsed millsecs");

        labelPanel.add(topicLabel);
        labelPanel.add(msgCountLabel);
        labelPanel.add(startTimeLabel);
        labelPanel.add(elapsedTimeLabel);

        stuff.add(labelPanel);
        stuff.add(foo);
        content.add(stuff);
        bar.setVisible(true);
    }

    public void initialize(int init_value, MqttClient mqtt, String topic) {

        //GridLayout lo = new GridLayout(1,4);
        counterField.setColumns(10);
        counterField.setEditable(false);
        startTimeField.setColumns(10);
        startTimeField.setEditable(false);
        elapsedTimeField.setColumns(10);
        elapsedTimeField.setEditable(false);
        topicLabel.setAlignmentX(LEFT_ALIGNMENT);
        FontMetrics fm = topicLabel.getFontMetrics(topicLabel.getFont());
        topicLabel.setPreferredSize(new Dimension(10 * fm.charWidth('A'), fm.getHeight()));
        counterField.setText(new Integer(init_value).toString());
        startTimeField.setText("");
        elapsedTimeField.setText(new Integer(0).toString());
        topicLabel.setText(topic);
        add(topicLabel);
        add(counterField);
        add(startTimeField);
        add(elapsedTimeField);
        this.mqtt = mqtt;
        subscribe(topic, (byte) 0);
        register();

    }


    private void register() {
        mqtt.setCallback(this);
    }

    protected void subscribe(String topic, byte qos) {

        try {
            String[] theseTopics = new String[1];
            byte[] theseQoS = new byte[1];

            theseTopics[0] = topic;
            theseQoS[0] = qos;
            mqtt.subscribe(theseTopics);

        } catch (MqttException ex) {
            ex.printStackTrace();
            logger.error(ex.toString());
        }
    }

    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MsgCounterField");

    private MqttClient mqtt = null;
    private int counter = 1;
    private JLabel topicLabel = new JLabel();
    private JTextField counterField = new JTextField();
    private JTextField startTimeField = new JTextField();
    private JTextField elapsedTimeField = new JTextField();
    private boolean firstMessageReceived = false;
    GregorianCalendar start = null;
    long t1 = 0;

    public void connectionLost(Throwable arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
        counterField.setText(new Integer(counter++).toString());
        if (firstMessageReceived == false) {
            firstMessageReceived = true;
            start = new GregorianCalendar();
            startTimeField.setText(start.get(Calendar.HOUR) + ":" + start.get(Calendar.MINUTE) + ":" + start.get(Calendar.SECOND) + ":" + start.get(Calendar.MILLISECOND));
            t1 = start.getTimeInMillis();
        }
        GregorianCalendar now = new GregorianCalendar();
        long t2 = now.getTimeInMillis();
        elapsedTimeField.setText(new Long(t2 - t1).intValue() + "");

    }

}
