package com.codeondemand.javapeppers.poblano.mqtt.ui;

import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.FontMetrics;
import java.awt.GridLayout;
import java.util.Properties;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.eclipse.paho.client.mqttv3.MqttClient;

import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import com.codeondemand.javapeppers.poblano.mqtt.base.MqttBrokerInfo;
import com.codeondemand.javapeppers.poblano.mqtt.base.MqttConnector;

public class MqttMessageCounterPanel {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MqttMessageCounterPanel foo = new MqttMessageCounterPanel();
		foo.initialize(args[0]);
	}
	
	public void initialize(String pfile){
		// Open the properties file that contains all of 
		// the configuration information
		Properties app_properties = MiscUtil
				.loadXMLPropertiesFile(pfile);

		// Set up our initial connection information
		MqttBrokerInfo brokerInfo = new MqttBrokerInfo("CCAOE", app_properties
				.getProperty("brokerip"), app_properties
				.getProperty("brokerport"));
		MqttConnector c = new MqttConnector();
		@SuppressWarnings("unused")
		MqttClient mqtt = c.addConnection("foo", "msgCounter", brokerInfo);
		
		// Pull in the list of topics from the properties file
		String[] topics = app_properties.getProperty("topics").split(":");	
		int count = topics.length;
		
		JFrame frame = new JFrame( "MQTT Counter");
		Container content = frame.getContentPane();
		
		FlowLayout flo = new FlowLayout();
		JPanel stuff = new JPanel(flo);
				
		GridLayout mlo = new GridLayout(1,4);
		JPanel labelPanel = new JPanel(mlo);
		JLabel topicLabel = new JLabel();
		topicLabel.setAlignmentX(java.awt.Component.LEFT_ALIGNMENT);
		JLabel msgCountLabel   = new JLabel();
		JLabel startTimeLabel   = new JLabel();
	    JLabel elapsedTimeLabel = new JLabel();
		
		FontMetrics fm = topicLabel.getFontMetrics(topicLabel.getFont());
		topicLabel.setPreferredSize(new Dimension(12*fm.charWidth('A'),fm.getHeight()));
		msgCountLabel.setPreferredSize(new Dimension(12*fm.charWidth('A'),fm.getHeight()));
		startTimeLabel.setPreferredSize(new Dimension(12*fm.charWidth('A'),fm.getHeight()));
		elapsedTimeLabel.setPreferredSize(new Dimension(12*fm.charWidth('A'),fm.getHeight()));
		topicLabel.setText("Topic name");
		msgCountLabel.setText("msg count");
		startTimeLabel.setText("start time");
		elapsedTimeLabel.setText("elapsed time");
		
		labelPanel.add(topicLabel);
		labelPanel.add(msgCountLabel);
		labelPanel.add(startTimeLabel);
		labelPanel.add(elapsedTimeLabel);
		
		GridLayout lo = new GridLayout(count,1);
		JPanel counterPanel = new JPanel(lo);
		MsgCounterField[] foo = new MsgCounterField[count];
		MqttClient[] mqtts = new MqttClient[count];
		for( int i = 0; i < count ; i++ ){
			foo[i] = new MsgCounterField();
			foo[i].setSize(50, 20);
			mqtts[i]= c.addConnection("foo"+i, "counter"+i, brokerInfo);
			foo[i].initialize(0, mqtts[i], topics[i]);
			counterPanel.add(foo[i]);
		}
		
		frame.setSize(500, count * 40);
		frame.setLocation(100,100);
		
		stuff.add(labelPanel);
		stuff.add(counterPanel);
		content.add(stuff);
		frame.setVisible(true);
		
	}

}
