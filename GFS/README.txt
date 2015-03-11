Team Members:
Chandhya Thirugnanasambantham - 2021158116
Sharath Pujar

CONTRIBUTUION OF TEAM MEMBERS:
Chandhya -  
Implementing  Load balancing for assigning chunks to server, replica creation,modifying existing design, Testing and debugging
Methods involved - Method generateServerNos(),performSort() to decide on which server needs to assigned 
based on file chunks and totalspace occupied by chunks.
Also other changes/updates to existing code in listener threads, corresponding to replica creation 

Sharath -
Implementing crash handling and transferring chunks to servers after crash failure, Testing and debugging 
Methods to handle crash failure and deciding on chunks
Other changes/updates to existing code in listener thread, integrating with existing crash handling implementation.

EXECUTION INSTRUCTIONS:
javac *.java - For compiling all files
For client, input needs to be specified as follows:
java Node input.txt
Other servers can be run with the command - java Node

 