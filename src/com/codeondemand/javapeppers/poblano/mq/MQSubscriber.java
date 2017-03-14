// --------------------------------------------------------------------------
//  javapeppers Confidential
//  
//
// --------------------------------------------------------------------------
package com.codeondemand.javapeppers.poblano.mq;



import com.ibm.mq.*;


public class MQSubscriber implements Runnable{

    //***********************************************************************
    // Constructors
    //***********************************************************************
    public MQSubscriber( ){
    }

    //***********************************************************************
    // Public methods and data
    //***********************************************************************

    public void initialize(String qm, String host, int port,
                           String channel, String queue, MQListener listener ){

        this.listener = listener;

        // Set up the getter object and connect
        
        MQEnvironment.hostname  = host;
        MQEnvironment.port      = port;
        MQEnvironment.channel   = channel;
        getter = new MQGetter(  );
        if( !getter.connect( qm, queue ) ){
            System.err.println( classname + ": Failed to conect to MQ." );
        }
    }

    public void run(){
        try{
            while( true ){
                boolean readmore = true;
                while( readmore ){
                    MQMessage mq_msg = getter.getMessage();
                    if( mq_msg != null ){
                        String msg = MQGetter.getTextPayload(mq_msg);
                        listener.incomingMessage(msg);
                    }else{
                        readmore = false;
                    }
                }
                Thread.sleep(500);
            }
        }catch(InterruptedException ie){
        	ie.printStackTrace();
        }
    }




    //***********************************************************************
    // Private data and methods
    //***********************************************************************
    protected     MQGetter     getter    = null;
    private       MQListener listener  = null;
    private final String          classname = getClass().getName();
}


