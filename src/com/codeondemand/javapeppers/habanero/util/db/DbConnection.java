/**
 * 
 */
package com.codeondemand.javapeppers.habanero.util.db;

import java.sql.Connection;

/**
 *  This is a wrapper class which allows a Connection object to be used a
 *  a number of times.  This is an experiment in dealing with a memory leak
 *  problem with the JDBC driver.  It may be used for other things such 
 *  as maintaining a count of the number of objects which are currently 
 *  using the connection.
 * 
 *
 *  @author gfa
 *
 *  @version  $Id: $
 */

public class DbConnection {

	//***********************************************************************
	// Constructors
	//***********************************************************************
	/**
	 * @param c The java.sql.Connection object to use for request.
	 */
	public DbConnection(Connection c) {
		connection = c;
	}

	//***********************************************************************
	// Public methods and data
	//***********************************************************************
	/**
	 * Decrements the use count for the Connection object and returns the new
	 * count.
	 *
	 * @return The decremented used count.
	 */
	public int decrementUseCount() {
		useCount -= 1;
		return useCount;
	}

	/**
	 * Returns the Connection object being used.
	 *
	 * @return The Connection object being used.
	 *
	 */
	public Connection getConnection() {
		return connection;
	}

	/**
	 * Returns the current use count.
	 *
	 * @return The current use count.
	 */
	public int getUseCount() {
		return useCount;
	}

	/**
	 * Sets the Connection object for this class.
	 *
	 * @param c The Connection object to use.
	 */
	public void setConnection(Connection c) {
		connection = c;
	}

	/**
	 * Allows setting of the use count.
	 *
	 * @param count The new use count to use.
	 */
	public void setUseCount(int count) {
		useCount = count;
	}

	//***********************************************************************
	// Protected methods and data
	//***********************************************************************

	//***********************************************************************
	// Private data and methods
	//***********************************************************************
	private int useCount = 10000;
	private Connection connection = null;
}
