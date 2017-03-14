package com.codeondemand.javapeppers.aleppo.writer;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

/**
 * This class writes to an MQTT compliant broker. The broker infomation needs to
 * be set in the configuration file.
 * 
 * @author gfa
 * 
 */
public class MqttWriter extends DestinationWriter {

	@Override
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

	@Override
	public boolean reset() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
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

			// System.out.println((String)pmap.get("initmsg"));
		}
	}

	@Override
	public boolean write(Object data) {
		if (!initialized) {
			initialized = doInitialization();
		}
		boolean retval = false;
		//System.out.println(data.getClass().getCanonicalName());
		if (data instanceof String && data.toString().trim().length() == 0) {
			retval = true;
		} else {
			try {
				byte[] payload = null;
				String split = "";
				if (data instanceof RecordCapsule && ((RecordCapsule) data).getField("split") != null) {
					split = ((RecordCapsule) data).getField("split").getData().toString();
					((RecordCapsule) data).removeField("split");

					// System.err.println(split);
				}
				if (pmap.containsKey("topic.field")) {
					if (!((RecordCapsule) data).getField((String) pmap.get("topic.field")).isNull()) {
						topics[0] = ((RecordCapsule) data).getField((String) pmap.get("topic.field")).getData()
								.toString();
					}
				}
				if (pmap.containsKey("field")) {
					String fields = pmap.get("field").toString();
					ArrayList<String> list = MiscUtil.StringToList(fields, "|");
					if (list.size() > 1) {
						Iterator<String> it = list.iterator();
						StringBuffer foo = new StringBuffer();
						while (it.hasNext()) {
							String field = (String) it.next();
							String temp = ((RecordCapsule) data).getField(field).getData().toString();
							foo.append(temp + "|");
						}
						payload = foo.toString().getBytes();
						// System.out.println(foo.toString());
					} else {
						RecordCapsule temp = (RecordCapsule) data;
						if (temp.checkField((String) pmap.get("field"))) {
							Object d = temp.getField((String)pmap.get("field")).getData();
							if (d instanceof byte[]) {
								payload = (byte[]) d;
							} else {
								if ( d != null ){
									String foo = d.toString();
									if( foo != null){
										payload = foo.getBytes();																			
									}
								}
							}
						}
					}
				} else {
					if (data instanceof RecordCapsule) {
						RecordCapsule temp = (RecordCapsule) data;
						StringBuffer foo = new StringBuffer();
						for (int i = 0; i < temp.getFieldCount(); i++) {
							if (!temp.getField(i).isNull()) {
								foo.append(temp.getField(i).getData().toString() + "|");
							} else {
								foo.append("|");
							}
						}
						payload = foo.toString().getBytes();
					} else {
						// System.out.println( data);
						if( data != null){
							payload = data.toString().getBytes();							
						}
					}
				}

				//
//				System.out.println( new String(payload));
				mqtt_pub.publish(topics[0] + split, payload, bqos[0], false);
				retval = true;
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return retval;
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

		//System.out.println(client);
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

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttWriter");

	protected MqttClient mqtt_pub = null;
	protected byte[] bqos = new byte[1];
	protected String[] topics = new String[1];
	protected boolean initialized = false;

}
