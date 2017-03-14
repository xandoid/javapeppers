//  javapeppers Confidential
//  
// --------------------------------------------------------------------------
package com.codeondemand.javapeppers.poblano.mq;


/**
* This interface provides a common mechanism for message subscriber
* components to be notified when a message is received.
*
*/
public interface MQListener{

    //***********************************************************************
    // Interface methods
    //***********************************************************************

    /**
    * Returns the text payload from an MQ message
    *
    * @param message  The text payload of an MQ message.
    */
    public void incomingMessage( String message);

}