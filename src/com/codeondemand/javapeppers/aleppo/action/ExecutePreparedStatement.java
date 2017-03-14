package com.codeondemand.javapeppers.aleppo.action;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import com.codeondemand.javapeppers.aleppo.common.DataCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordCapsule;
import com.codeondemand.javapeppers.aleppo.common.RecordProcessor;
import com.codeondemand.javapeppers.habanero.util.db.DbAccessManager;

public  class ExecutePreparedStatement extends RecordProcessor {

	@Override
	public RecordCapsule processRecord(RecordCapsule input) {
		RecordCapsule retval= input;
		ArrayList<String> params = new ArrayList<String>();
		for( int i = 0; i < fields.size(); i++){
			params.add("%"+input.getField(fields.get(i)).getData().toString()+"%");			
		}
		setParameters( params);
		if( input.getField(output.get(0)).getData() == null ||
			input.getField(output.get(0)).getData().toString().equals("")){
			executeStatement( input);			
		}
		return retval;
	}

	@Override
	public  boolean doInitialization(){
		boolean retval = true;
		if( pmap.get("keypfx") != null){
			keypfx= (String) pmap.get("keypfx");
		}
		
		String dburl = props.getProperty(keypfx + "db.url"); //$NON-NLS-1$
		String dbuser = props.getProperty(keypfx + "db.uid"); //$NON-NLS-1$
		String dbpwd = decryptPWD(props.getProperty(keypfx + "db.pwd")); //$NON-NLS-1$
		
		String driver = props.getProperty(keypfx + "db.driver"); //$NON-NLS-1$
		DbAccessManager dbmgr = new DbAccessManager(dburl, 1, dbuser, dbpwd);
		dbmgr.registerDriver(driver);
		
		String query = props.getProperty(keypfx+"db.query");
		
		Connection con = dbmgr.getConnection(dburl);
		if( con != null ){
			try {
				stmt =con.prepareStatement(query);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if( pmap.containsKey("fields")){
			StringTokenizer stok = new StringTokenizer( (String) pmap.get("fields"), "|");
			while( stok.hasMoreTokens()){
				fields.add(stok.nextToken());				
			}
		}
		
		if( pmap.containsKey("output")){
			StringTokenizer stok = new StringTokenizer( (String) pmap.get("output"), "|");
			while( stok.hasMoreTokens()){
				output.add(stok.nextToken());				
			}
		}
		return retval;
	}

	protected  boolean setParameters(ArrayList<String> p){
		boolean retval = false;
		try {
			
			for( int i = 0; i < p.size() ; i++){
				stmt.setString(i+1, p.get(i) );				
			}
			retval = true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		return retval;
	}
	
	protected  boolean executeStatement(RecordCapsule input){
		boolean retval = true;
		try {
			stmt.execute();
			ResultSet rs = stmt.getResultSet();
			if( rs.next()){
				input.getField(output.get(0)).setData(rs.getString(1));
				input.getField(output.get(1)).setData(rs.getString(2));
				if( rs.next()){
					StringBuffer sb = new StringBuffer();
					do{
						sb.append(rs.getString(1)+":"+rs.getString(2)+"|");
					}while( rs.next());
					input.addDataCapsule(new DataCapsule("extra",sb.toString()), false);
				}else{
					input.addDataCapsule(new DataCapsule("extra",""), false);					
				}
			}else{
				input.getField(output.get(0)).setData("UNKNOWN");
				input.getField(output.get(1)).setData("UNKNOWN");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retval;
	}
	
	protected String decryptPWD(String input) {
		String retval = input;
		
		// Not doing anything for now.
		
		return retval;
	}
	
	protected Connection con = null;
	protected String proc = null;
	protected PreparedStatement stmt = null;
	protected String keypfx = null;
	protected ArrayList<String> fields = new ArrayList<String>();
	protected ArrayList<String> output = new ArrayList<String>();
	
	@Override
	public void done() {
		// TODO Auto-generated method stub
		
	}


}
