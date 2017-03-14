/**
The com.codeondemand.javapeppers.aleppo.transform package provides some basic transform classes
as well as providing the base class that all need to extend if they intend
to be used in the Aleppo framework.  The base class is RecordTransform.

The package should provide the functionality that is needed for a number of
transformation tasks. such as
<p>
<ul>
<li>XSLT Transformation 
<li>Case conversion of fields
<li>Sanitizing some of the fields being processed to anonymize the data
<li>Token substitution for the fields to allow some basic translation operations.
</ul>

<p>
Like all components of Aleppo that are intended to be executed by the standard
Aleppo runtime application (com.codeondemand.javapeppers.aleppo.process.ProcessLauncher), their 
behavior is controlled by a specification used in the dataflow configuration file.
The tag used for a transform consists of a transform, along with a class parameter
specifying the particular transform class and supplemental parameters that may be
needed by the transform to perform its functionality.  As always, the parameters
are available via the 'pmap' variable that is available to all classes that extend
the RecordTransform class. pmap is of type TreeMap with the parameter name as the 
key.
<pre>
 	&lt;dataflow&gt;
  	    ... (source of data flow)
  	    &lt;process&gt;
            &lt;transform class="com.codeondemand.javapeppers.aleppo.transform.XXXX"
                          param1="yyyyy" param2="zzzz" /&gt;
        &lt;process/&gt;
        ... (destination of data flow)
    &lt;dataflow/&gt;
</pre>
 
*/
package com.codeondemand.javapeppers.aleppo.transform;