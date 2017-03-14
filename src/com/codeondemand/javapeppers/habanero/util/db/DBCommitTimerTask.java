package com.codeondemand.javapeppers.habanero.util.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.TimerTask;

public class DBCommitTimerTask extends TimerTask {

	public DBCommitTimerTask(Connection con) {
		myCon = con;
	}

	@Override
	public void run() {
		try {
			if (myCon != null && !myCon.isClosed()) {
				synchronized (myCon) {
					myCon.commit();
					// System.out.println( "Timer committed");
					done = true;
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public synchronized boolean isDone() {
		return done;
	}

	protected Connection myCon = null;
	protected boolean done = false;
}
