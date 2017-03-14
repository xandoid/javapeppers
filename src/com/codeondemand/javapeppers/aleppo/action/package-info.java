/**
 * The com.codeondemand.javapeppers.aleppo.action package contains classes that tend to encapsulate behavior that
 * interact with external systems during the processing of a dataflow.  
 * 
 * Most classes in this package extend the com.codeondemand.javapeppers.aleppo.common.RecordProcessor class. 
 * There is also a generalized extension to allow for arbitrary Runnable classes to
 * be handled by the framework.  They need only implement a couple of functions that allow
 * the data context to be passed from the framework.
 * 
 * @author Gary Anderson
*/
package com.codeondemand.javapeppers.aleppo.action;