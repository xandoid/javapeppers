package com.codeondemand.javapeppers.aleppo.reader;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;

import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;

/**
 * This class reads from an MQTT type broker and passes the message in a
 * DataCapsule named 'data' (this should be parameterized).
 * 
 * @author gfa
 * 
 */
public class MqttReader extends SourceReader implements MqttCallback {

	@Override
	public boolean close() {
		// TODO Auto-generated method stub
		try {
			if (mqtt_sub != null) {
				mqtt_sub.disconnect();
			}
		} catch (MqttPersistenceException e) {
			logger.error(e.toString());
		} catch (MqttException e) {
			logger.error(e.toString());
		}
		return false;
	}

	@Override
	public boolean reset() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public Object read() {
		//myThread = Thread.currentThread();
		while (input.isEmpty()) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//Thread.yield();
		}
		return input.poll();
	}

	@Override
	public boolean doInitialization() {
		boolean retval = false;

		MqttConnectOptions opts = new MqttConnectOptions();
		opts.setCleanSession(true);

		String[] topics = new String[1];
		String client = null;
		topics[0] = "test";
		String mqtt_host = "localhost";
		int mqtt_port = 1883;
		bqos = new byte[1];
		bqos[0] = (byte) 0;

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

		opts.setCleanSession(true);
		if( pmap.containsKey("clean")){
			boolean clean = Boolean.parseBoolean((String)pmap.get("clean"));
			opts.setCleanSession(clean);
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
		if( pmap.containsKey("mqtt.uid")){
			uid = (String)pmap.get("mqtt.uid");
			opts.setUserName(uid);
		}
		if( pmap.containsKey("mqtt.pwd")){
			pwd = (String)pmap.get("mqtt.pwd");
			if( pmap.containsKey("mqtt.pwd_encrypted")){
				pwd = MiscUtil.decodeB64String(pwd);
			}
			opts.setPassword(pwd.toCharArray());
		}
		String cstring = "tcp://" + mqtt_host + ":" + mqtt_port;
		try {
			mqtt_sub = new MqttClient(cstring,client);
		  	
			mqtt_sub.setCallback(this);
			mqtt_sub.connect(opts);
			//mqtt_sub.subscribe(topics);
			subscribe( topics[0],bqos[0]);
		
			retval = true;
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return retval;
	}

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttReader");

	// ***********************************************************************
	// Public methods and data
	// ***********************************************************************

	/**
	 * Requests a subscription to the message broker . You must have passed a
	 * MqttClient object in one of the constructors for this to work.
	 * 
	 * @param topic
	 *            The topic that should be subscribed.
	 * @param qos
	 *            The quality of service for the subscription (0,1,2)
	 */
	protected void subscribe(String topic, byte qos) {

		try {
			String[] theseTopics = new String[1];
			byte[] theseQoS = new byte[1];

			theseTopics[0] = topic;
			theseQoS[0] = qos;
			mqtt_sub.subscribe(theseTopics);

		} catch (MqttException ex) {
			ex.printStackTrace();
			logger.error(ex.toString());
		}
	}

	/**
	 * Requests a subscription be terminated.
	 * 
	 * @param topic
	 *            The topic that should be unsubscribed.
	 */
	public void unsubscibe(String topic) {

		try {
			String[] theseTopics = new String[1];

			theseTopics[0] = topic;
			mqtt_sub.unsubscribe(theseTopics);

		} catch (MqttException ex) {
			ex.printStackTrace();
			logger.error(ex.toString());
		}
	}

	// ***********************************************************************
	// Protected methods and data
	// ***********************************************************************

	protected byte[] bqos = null;
	/** The MqttClient object that is connected to the broker. */
	protected MqttClient mqtt_sub = null;
	protected Object message = null;
	//protected Thread myThread = null;
	protected boolean suspended = false;
	protected int maxQueueBuffer = 10000;
	protected int minQueueBuffer = 500;
	protected String uid = "";
	protected String pwd = "";
	protected ConcurrentLinkedQueue<String> input = new ConcurrentLinkedQueue<String>();

	public void connectionLost(Throwable arg0) {
		System.err.println( "Connection lost.");
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public  void messageArrived(String topic, MqttMessage m) throws Exception {
		byte[] msg = m.getPayload();
		String message = new String(msg);
		logger.debug(message);
		if( input.size() < maxQueueBuffer){
			if( m != null){
				input.add( new String( m.getPayload()));
			}
		}else{
			//while( input.size() > minQueueBuffer){
			//	Thread.yield();
			//}
			System.err.println( "Max queue exceeded.");
		}
	}

}
