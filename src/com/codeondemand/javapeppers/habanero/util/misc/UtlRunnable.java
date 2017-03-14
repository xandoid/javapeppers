// --------------------------------------------------------------------------
//                
// --------------------------------------------------------------------------
package com.codeondemand.javapeppers.habanero.util.misc;

import java.util.Observable;


/**
*
* This is a simple wrapper class which allows a Runnable object to act as
* an Observable, allowing other Objects to act as Observers and be notified
* when it is done.  Note that this class does not provide any of the logic
* related to notification, which needs to be handled by other objects.
*
* It is not intended that this Object provide any useful functionality.
* You should subclass it to provide useful behavior.
*
*  @author gfa
*
*  @version  1.0
*/

public  class UtlRunnable
    extends  Observable
    implements Runnable{

    //***********************************************************************
    // Constructors
    //***********************************************************************
    public UtlRunnable(){
    }

    //***********************************************************************
    // Public methods and data
    //***********************************************************************
    public void run(){
    	//TO-DO: nothing
    }


    public Thread getThread(){
        return myThread;
    }
    //***********************************************************************
    // Protected methods and data
    //***********************************************************************
    protected Thread myThread                = null;
    
    //***********************************************************************
    // Implementation for Observer interface
    //***********************************************************************

    //***********************************************************************
    // Private data and methods
    //***********************************************************************

}








