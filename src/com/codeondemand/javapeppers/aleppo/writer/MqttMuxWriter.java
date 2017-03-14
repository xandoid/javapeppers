package com.codeondemand.javapeppers.aleppo.writer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;
import com.codeondemand.javapeppers.poblano.mqtt.base.MqttPublisherThread;

/**
 * This class writes to an MQTT compliant broker. The broker information needs
 * to be set in the configuration file.
 * 
 * @author gfa
 * 
 */
public class MqttMuxWriter extends DestinationWriter {

	@Override
	public boolean close() {
		boolean retval = false;
		Iterator<String> it = pubmap.keySet().iterator();
		while (it.hasNext()) {
			MqttPublisherThread mqtt_pub = pubmap.get(it.next());
			if (mqtt_pub != null) {
				mqtt_pub.setFinished();
			
				retval = true;
			}
		}
		try {
			wrtr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retval;
	}

	@Override
	public boolean reset() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean write(Object data) {
		if (!initialized) {
			initialized = doInitialization();
		}

		//System.out.println( data.toString());
		
		boolean retval = false;
		// Class c = data.getClass();
		if (data instanceof String && data.toString().trim().length() == 0) {
			retval = true;
		} else {
			String payload = null;
			String split = "";
			if (data instanceof RecordCapsule
					&& ((RecordCapsule) data).getField("mqtt.split") != null) {
				split = ((RecordCapsule) data).getField("mqtt.split").getData()
						.toString();
				((RecordCapsule) data).removeField("mqtt.split");
				
			}
			if (pmap.containsKey("field")) {
				String fields = pmap.get("field").toString();
				ArrayList<String> list = MiscUtil.StringToList(fields, "|");
				if (list.size() > 1) {
					Iterator<String> it = list.iterator();
					StringBuffer foo = new StringBuffer();
					while (it.hasNext()) {
						String field = (String) it.next();
						String temp = ((RecordCapsule) data).getField(field)
								.getData().toString();
						foo.append(temp + "|");
					}
					payload = foo.toString();
				} else {
					Object d = ((RecordCapsule) data).getField(
							(String) pmap.get("field")).getData();
					if (d instanceof byte[]) {
						payload = new String((byte[]) d);
					} else {
						payload = d.toString();
					}
				}
			} else {
				if (data instanceof RecordCapsule) {
					RecordCapsule temp = (RecordCapsule) data;
					StringBuffer foo = new StringBuffer();
					for (int i = 0; i < temp.getFieldCount(); i++) {
						if (!temp.getField(i).isNull()) {
							foo.append(temp.getField(i).getData().toString()
									+ "|");
						} else {
							foo.append("|");
						}
					}
					payload = foo.toString();
				} else {
					// System.out.println( data);
					payload = data.toString();
				}
			}

			try {
				logger.debug("Writing: "+payload);
				
				wrtr.write(payload);
				wrtr.newLine();
				wrtr.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (pubmap.containsKey(split)) {
				MqttPublisherThread mqtt_pub = pubmap.get(split);

				mqtt_pub.setMsg(payload);
				retval = true;
			}
		}
		return retval;
	}

	public boolean doInitialization() {
		boolean retval = false;

	
		try {
			wrtr = new BufferedWriter(new FileWriter("log.txt"));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		if (pmap.containsKey("sleep_interval")) {
			sleep_interval= Long.parseLong((String) pmap.get("sleep_interval"));
		}

		int hostcount = 0;
		if (pmap.containsKey("host.count")) {
			hostcount = Integer.parseInt((String) pmap.get("host.count"));
		}

		for (int i = 0; i < hostcount; i++) {
			String spec = (String) pmap.get("mqtt.spec." + String.valueOf(i));
			StringTokenizer stok = new StringTokenizer(spec, "|");
			if (stok.countTokens() == 8) {
				String key = stok.nextToken();
				String client = stok.nextToken();
				String host = stok.nextToken();
				int port = Integer.parseInt(stok.nextToken());
				topics[0] = stok.nextToken();
				String qtmp = stok.nextToken();
				if (qtmp.equals("1")) {
					bqos[0] = (byte) 1;
				}
				if (qtmp.equals("2")) {
					bqos[0] = (byte) 2;
				}
				MqttConnectOptions opts = new MqttConnectOptions();
				opts.setCleanSession(true);
				opts.setUserName(stok.nextToken());
				opts.setPassword(stok.nextToken().toCharArray());
				String cstring = "tcp://" + host + ":" + port;
				try {
					MqttClient mqtt_pub = new MqttClient(cstring, client);
					mqtt_pub.connect(opts);
					MqttPublisherThread t = new MqttPublisherThread(mqtt_pub,
							topics[0], null, bqos[0]);
					t.setSleepInterval(sleep_interval);

					pubmap.put(key, t);
					Thread thread = new Thread(t);
					thread.start();
					retval = true;
				} catch (MqttException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return retval;
	}

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttMuxWriter");

	protected TreeMap<String, MqttPublisherThread> pubmap = new TreeMap<String, MqttPublisherThread>();

	protected byte[] bqos = new byte[1];
	protected String[] topics = new String[1];
	private boolean initialized = false;
	private BufferedWriter wrtr = null;
	private long sleep_interval = 0L;
	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}
}
