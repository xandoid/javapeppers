package com.codeondemand.javapeppers.poblano.mqtt.task;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.codeondemand.javapeppers.poblano.mqtt.base.MqttPublisherThread;

public class MqttFilePublisher extends MqttTaskNode {

	public MqttFilePublisher() {
	}

	public MqttFilePublisher(String name) {
		super(name);
	}

	public void initialize() {
		super.initialize();
		if (fields.get("file") != null) {
			publishFile(fields.get("file"));
		}
	}

	@Override
	public void register() {
		this.mqtt_sub.setCallback(this);
	}

	private void publishFile(String file) {
		MqttPublisherThread pt = null;

		try {
			File ifile = new File(file);
			BufferedReader rdr = new BufferedReader(new FileReader(ifile));
			if (rdr.ready()) {
				pt = new MqttPublisherThread(getPub_client(), getPub_topic(), rdr.readLine(), getQos());
				if (pt != null) {

					// Thread t = new Thread(pt);
					// t.start();
					while (rdr.ready()) {
						String foo = rdr.readLine();
						// System.out.println( ">>"+foo);
						pt.setMsg(foo);
						// Thread.yield();
					}
					pt.setMsg("EOF");
					// pt.setFinished();
				}
				disconnect();
				rdr.close();
			}
		} catch (Exception e) {
			logger.error(e.toString());
		}

	}

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttFilePublisher");

	@Override
	public void messageArrived(String topic, MqttMessage msg) {
		// TODO Auto-generated method stub

	}
}
