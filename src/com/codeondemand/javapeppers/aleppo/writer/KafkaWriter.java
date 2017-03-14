/**
 * 
 */
package com.codeondemand.javapeppers.aleppo.writer;

import java.util.Properties;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;

/**
 * The KafkaWriter class basically wraps a KafkaProducer class as a aleppo
 * writer component.
 * 
 * @author gfa
 * 
 */
public class KafkaWriter extends DestinationWriter {

	public boolean close() {
		return true;
	}

	public boolean reset() {
		return close();
	}

	public boolean write(Object record) {
		boolean retval = true;
		if (!initialized) {
			initialized = doInitialization();
		}
		if( record instanceof RecordCapsule ){
			RecordCapsule rc = (RecordCapsule)record;
			String key = null;
			String msg = null;
			if( rc.getField(keyfield) != null && !rc.getField(keyfield).isNull()){
				key = rc.getField(keyfield).getData().toString();
			}
			if( rc.getField(msgfield) != null && !rc.getField(msgfield).isNull()){
				msg = rc.getField(msgfield).getData().toString();
			}
			ProducerRecord<String, String> message = new ProducerRecord<String,String>(topic,key,msg);
		
			logger.debug("Sending: "+ message);
			kprod.send(message);
		}else{
			retval = false;
		}
		return retval;
	}

	public boolean doInitialization() {
		boolean retval = true;
		if (pmap.containsKey("keyfield")) {
			keyfield = (String) pmap.get("keyfield");
		} else {
			retval = false;
		}
		if (pmap.containsKey("msgfield")) {
			msgfield = (String) pmap.get("msgfield");
		} else {
			retval = false;
		}
		if (pmap.containsKey("brokerlist")) {
			brokerlist = (String) pmap.get("brokerlist");
		} else {
			retval = false;
		}
		if (pmap.containsKey("topic")) {
			topic = (String) pmap.get("topic");
		} else {
			retval = false;
		}
		
		if (retval) {
			Properties p = new Properties();
			p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist);
			p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class.getName());
			p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class.getName());
			kprod = new KafkaProducer<String, String>(p);
		}
		return retval;
	}

	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("KafkaWriter");

	protected KafkaProducer<String, String> kprod = null;
	protected boolean initialized = false;
	protected String keyfield = null;
	protected String msgfield = null;
	protected String brokerlist = null;
	protected String topic = null;
	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

}
