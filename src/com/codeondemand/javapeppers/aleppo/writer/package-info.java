/**
* The com.codeondemand.javapeppers.aleppo.writer package provides the functionality for outputting the 
* payload passing through the Aleppo framework to a variety of channels such as
* files, queues, databases, logs, etc.
* 
* The responsibility of classes in this package are to write the payload appearing
* at the end of the dataflow to a destination target.  Standard classes provide the
* ability to output to the following targets:
* 
*<p>
*<ul>
*<li> Echoing to the Console (stdout) 
*<li> A standard file
*<li> Inserting into a database table
*<li> Posting to a JMS Topic
*<li> Writing to a JMS Queue (in progress)
*<li> Writing to standard log4j log file
*<li> Displaying in a Java UI component
*<li> Sending the content in a mail message.
*<li> Dropping into the bit bucket
*<li> Creating a PDF file (in progress)
*</ul>
* 
* @author Gary Anderson
*/
package com.codeondemand.javapeppers.aleppo.writer;