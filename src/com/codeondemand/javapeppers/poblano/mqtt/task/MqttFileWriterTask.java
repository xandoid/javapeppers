package com.codeondemand.javapeppers.poblano.mqtt.task;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.GregorianCalendar;

import org.apache.logging.log4j.LogManager;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttFileWriterTask extends MqttTaskNode {

	public MqttFileWriterTask() {
		// TODO Auto-generated constructor stub
	}

	public MqttFileWriterTask(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	
	public void initialize(){
		super.initialize();
		if( fields.get("filename") != null ){
			try{		
				out = new FileWriter( new File(fields.get("filename")));
			}catch( IOException ioe){
				logger.error(ioe.toString());
			}
		}
	}
	
	protected void processPayload( byte[] data){
		try{
			GregorianCalendar foo = new GregorianCalendar();
			String msg = new String(data);
			System.out.println(msg);
			if( msg.equals("EOF")){
				System.out.println( "END OF FILE RECEIVED");
				out.flush();
				out.close();
				disconnect();
			}
			out.write(foo.getTimeInMillis()+":"+msg+"\n");
			//out.flush();
		}catch( IOException ioe){
			logger.error( ioe.toString());
		}
	}
	
	public void register(){
		this.mqtt_sub.setCallback(this);
	}
	
	private FileWriter out = null;
	@Override
	public void messageArrived(String topic, MqttMessage msg) {
		processPayload( msg.getPayload());
		
	}
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("MqttFileWriterTask");
}
