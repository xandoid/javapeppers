package com.codeondemand.javapeppers.habanero.util.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;

import com.codeondemand.javapeppers.habanero.util.misc.MiscUtil;


public class DbQueryApp extends DbApplication {

	public DbQueryApp() {
		// TODO Auto-generated constructor stub
	}

	public void initialize( String[] args){
		super.initialize( args );
		if( properties != null){
			query = properties.getProperty("query.string");			
		}
		if( query == null || query.length() == 0){
			query = MiscUtil.getConsoleInput(System.in, "Please enter query string");
		}else{
			logger.debug("Using specified query string: "+ query);
		}
	}
	
	public ResultSet runQuery(){
		ResultSet rs = null;
		
		try{
			Connection connection = db_mgr.getConnection(dburl,uid,pwd);
			Statement stmt = connection.createStatement();
			rs = stmt.executeQuery(query);
		}catch( Exception sqle){
			sqle.printStackTrace();
		}
		
		return rs;	
	}
	
	private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger("DbQueryApp");
	
	protected String query = null;
}
