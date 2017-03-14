package com.codeondemand.javapeppers.aleppo.observer;

import java.util.Observable;
import java.util.Observer;

import com.codeondemand.javapeppers.aleppo.common.FlowNode;

public class NullObserver extends FlowNode implements Observer {
	
	public void isObservable() {
	}

	public void update(Observable arg0, Object arg1) {
		// this is a passive observer - lol
	}


	public boolean doInitialization() {
		// TODO Auto-generated method stub
		return true;
	}

}
