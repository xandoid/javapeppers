package com.codeondemand.javapeppers.poblano.test;

import com.codeondemand.javapeppers.poblano.mqtt.MqttTaskLauncher;


public class MqttTest extends MqttTaskLauncher{

	public MqttTest( String name){
		//TO-DO: Nothing to do here.
	}
	
	public void initialize( ){;	
		super.initialize("poblano/resources/application_properties.xml");
		System.out.println( "Finished");
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

  	    MqttTest foo = new MqttTest(args[0]);
	    foo.initialize();
	}

}
