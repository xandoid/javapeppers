package com.codeondemand.javapeppers.poblano.mqtt.task;

import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttMessage;



public class MqttLoggerTask extends MqttTaskNode {

	public MqttLoggerTask() {
		// TODO Auto-generated constructor stub
	}

	public MqttLoggerTask(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

	protected void processPayload( byte[] data){
		logger.info(new String(data));
	}
	
	public void register(){
		this.mqtt_sub.setCallback(this);
	}
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttLoggerTask");
	
	@Override
	public void messageArrived(String topic, MqttMessage msg) {
		processPayload( msg.getPayload());
	}
	
}
