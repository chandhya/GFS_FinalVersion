import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class ListeningThread extends Thread implements Runnable{
	Socket socket=null;
	ObjectInputStream ois;
	ObjectOutputStream oos;
	Message m;
	Socket clientSocket;
	ObjectOutputStream out;
	ObjectInputStream iis;
	int NodeNo;
	public ListeningThread(Socket socket, ObjectInputStream iis)
	 {
		this.socket = socket;
		this.NodeNo = Node.getNodeNumber(socket);
		this.ois = iis;
	}
	@Override
	public void run(){
		while(true)
		{
			try {
				m = (Message)ois.readObject();
				if(m.mt.equals(MessageType.WRITEREQ))
				{
				if(Node.currentNode.getHostName().equals(Node.MSERVER))
				{
					List<Integer> serverNos = new ArrayList<Integer>();
					int cServerNo=4;
					boolean ifPresent=false;
					if(Node.directory.isEmpty()) {
					while(true){
					int oldValue=(int)Node.generateRandvalue(Node.initialServerNo, Node.initialServerNo+Node.serverCount);
					for(int y=0;y<Node.otherNodes.size();y++){
						if(Node.otherNodes.get(y)==oldValue)
							{
							ifPresent=true;
							break;
							}
					}
					if(!ifPresent)
					{
						serverNos.add(oldValue);
						break;
					}
					}
					//System.out.println("Server chosen- "+serverNos.get(0));
						while(serverNos.size()<Node.replicaCount) {
							cServerNo = (int)Node.generateRandvalue(Node.initialServerNo, Node.initialServerNo+Node.serverCount);
							if(!serverNos.contains((int)cServerNo) && !Node.otherNodes.contains((int)cServerNo))
							serverNos.add(cServerNo);
						}
						
					}
					else{
						serverNos.clear();
						System.out.println("Server No size "+serverNos.size());
						for(int t=Node.initialServerNo;t<Node.initialServerNo+Node.serverCount;t++)
						{
							System.out.println("gets here!");
							
								if(!Node.directory.containsKey(t)&&!Node.otherNodes.contains(t))
								{serverNos.add(t);
								System.out.println("Adding value at create"+t);
								
								}
								
							
							
						}
						serverNos.addAll(Node.generateServerNumbers());
						//serverNos = Node.generateServerNumbers();
					}
					for(int q=0;q<Node.replicaCount;q++)
					{
						cServerNo=serverNos.get(q);
						System.out.println("Server Nos "+cServerNo);
						
					boolean alreadyConnected=false;
					if(!Node.connectedNodes.isEmpty())
					for(int i=0;i<Node.connectedNodes.size();i++){
						if(Node.getNodeNumber(Node.connectedNodes.get(i))==cServerNo)
						{
							alreadyConnected=true;
							//System.out.println("Write req- spotted server to send data");
							break;
						}
					}
					String cServerStr;
					if(cServerNo<10)
						{
						cServerStr = "0"+String.valueOf(cServerNo);
						//System.out.println("Server num is appropriately displayed - "+cServerStr);
						}
					else
						cServerStr = String.valueOf(cServerNo);
					
					if(!alreadyConnected)
					 	while (true) {
							try {
								//clientSocket = new Socket("dc0" + Node.jFromFile[i] + ".utdallas.edu",Node.SERVER_PORT);
								
								clientSocket = new Socket("dc" + cServerStr + ".utdallas.edu",Node.SERVER_PORT);
								if (clientSocket == null) 
								{
								Thread.sleep(1000);
								} else 
								{
								System.out.println("Client made connection with "+clientSocket.getInetAddress().getHostName());
								Node.connectedNodes.add(clientSocket);
								out = new ObjectOutputStream(clientSocket.getOutputStream());
								Node.outMap.put(Node.getNodeNumber(clientSocket),out);
								iis = new ObjectInputStream(clientSocket.getInputStream());
								ListeningThread lt = new ListeningThread(clientSocket,iis);
								lt.start();
								break;
								}
								} 
								catch (Exception e) {
								//e.printStackTrace();
								}
							}
				}
					for(int i=0;i<Node.replicaCount;i++){
						int cServerNoTemp = serverNos.get(i);
						System.out.println("Write req now being sent to server "+cServerNoTemp);
					SendingThread st = new SendingThread(cServerNoTemp,MessageType.WRITEREQ,m.fname,m.fileContent);
					st.start();
					}
				}
				else
				{
					File file = new File(m.fname);
					file.createNewFile();
					
					FileWriter fw = new FileWriter(file.getAbsoluteFile());
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(m.fileContent);
					bw.close();
					File file2 = new File(m.fname);
					//System.out.println("File created is "+m.fname+" at Server "+Node.currentNode.getHostName().charAt(3));
					long filespace = file2.length();
					String fileSize = String.valueOf(filespace);
					SendingThread st = new SendingThread(Node.MServerNo,MessageType.HEARTBEAT,m.fname,fileSize);
					st.start();
					
					
				}
				}
				
				 else if(m.mt.equals(MessageType.APPEND))
				{
				if(Node.currentNode.getHostName().equals(Node.MSERVER))
				{
					ArrayList<Entry> fileChunksTemp2 = new ArrayList<Entry>();
					int lastchunkNo=0;long filesize = 0;int serverNo=4;int tempchunkNo;
					List<Integer> serverNos = new ArrayList<Integer>();
					for(int i=Node.initialServerNo;i<Node.initialServerNo+Node.serverCount;i++) {
						
						if(Node.directory.get(i)!=null &&!Node.directory.get(i).isEmpty())
						{
							fileChunksTemp2=Node.directory.get(i);
						for(int j=0;j<fileChunksTemp2.size();j++) {
							
							String fnametemp= m.fname.substring(0,m.fname.length()-4);
							
							//System.out.println("File name -"+fnametemp);
							
							String fnameTemp2=fileChunksTemp2.get(j).fname.substring(0, fileChunksTemp2.get(j).fname.length()-8);
							//System.out.println("File name"+fnameTemp2);
							if(fnameTemp2.equals(fnametemp))
							{
								//System.out.println("Found match - "+fnametemp);
								tempchunkNo =Integer.parseInt(Character.toString(fileChunksTemp2.get(j).fname.charAt(fileChunksTemp2.get(j).fname.length()-7)));
								//System.out.println("file's chunk no "+tempchunkNo);
								if(lastchunkNo<tempchunkNo){
									lastchunkNo =tempchunkNo;
								//	System.out.println("new Chunk No -"+lastchunkNo);
									filesize = fileChunksTemp2.get(j).fileSize;
									//serverNo=i;
									//serverNos.add(i);
									
								}
							}
						}
						}
						
					}
					/*Newly Added*/
					//System.out.println("HIghest chunk so far"+lastchunkNo);
					for(Integer serverNum:Node.directory.keySet()) {
						fileChunksTemp2=Node.directory.get(serverNum);
							for(int j=0;j<fileChunksTemp2.size();j++) {
							
							String fnametemp= m.fname.substring(0,m.fname.length()-4);
							
							//System.out.println("File name -"+fnametemp);
							
							String fnameTemp2=fileChunksTemp2.get(j).fname.substring(0, fileChunksTemp2.get(j).fname.length()-8);
							//System.out.println("File name"+fnameTemp2);
							if(fnameTemp2.equals(fnametemp))
							{
								//System.out.println("Found match - "+fnametemp);
								tempchunkNo =Integer.parseInt(Character.toString(fileChunksTemp2.get(j).fname.charAt(fileChunksTemp2.get(j).fname.length()-7)));
								//System.out.println("file's chunk no "+tempchunkNo);
								if(lastchunkNo==tempchunkNo){
									lastchunkNo =tempchunkNo;
								//	System.out.println("new Chunk No -"+lastchunkNo);
									//filesize = fileChunksTemp2.get(j).fileSize;
									serverNo=serverNum;
									serverNos.add(serverNum);
									
								}
							}
						}
					}
					/**/
					//for(int y=0;y<Node.replicaCount;y++)
					//System.out.println("serverNo -"+serverNo);
					String givenFile=m.fname;
					String newFile;
					String contents =m.fileContent;
					if(8192-filesize<m.fileContent.length())
						{
					//	lastchunkNo++;
						//m.fname = m.fname.substring(0,5)+"_"+lastchunkNo+"NN_"+serverNo+".txt";
						//System.out.println("new file generation for this append?");
						for(int k=0;k<Node.replicaCount;k++)
						{
							if(k<serverNos.size()){
							newFile=givenFile.substring(0,givenFile.length()-4)+"_"+lastchunkNo+"_"+serverNos.get(k)+".txt"+"NN";
						//System.out.println("File bein created #NN"+newFile);
						SendingThread st = new SendingThread(Node.getNodeNumber(socket),MessageType.MSERVRESP,newFile,contents);
						st.start();
						Thread.sleep(1000);
							}
						}
						serverNos.clear();
						System.out.println("Server No size "+serverNos.size());
						for(int t=Node.initialServerNo;t<Node.initialServerNo+Node.serverCount;t++)
						{
							
							if(!Node.directory.containsKey(t) && !Node.otherNodes.contains(t))
							{serverNos.add(t);
							System.out.println("Adding value at append "+t);
							
							}
							
						}
						serverNos.addAll(Node.generateServerNumbers());
						
						for(int h=0;h<serverNos.size();h++){
							System.out.println("Server Nos employed "+serverNos.get(h));
						}
						//serverNos = Node.generateServerNumbers();
						for(int k=0;k<Node.replicaCount;k++)
						{
							newFile=givenFile.substring(0,givenFile.length()-4)+"_"+(lastchunkNo+1)+"_"+serverNos.get(k)+".txtN";
						//System.out.println("File bein created #N"+newFile);
						SendingThread st2 = new SendingThread(Node.getNodeNumber(socket),MessageType.MSERVRESP,newFile,contents);
						st2.start();
						Thread.sleep(1000);
						}
						//System.out.println("Its either this - "+m.fname);
						}
				
					else{
						for(int k=0;k<Node.replicaCount;k++)
						{
							if(k<serverNos.size())
							{
							newFile=givenFile.substring(0,givenFile.length()-4)+"_"+lastchunkNo+"_"+serverNos.get(k)+".txt";
							//System.out.println("File bein created #"+newFile);
						SendingThread st2 = new SendingThread(Node.getNodeNumber(socket),MessageType.MSERVRESP,newFile,contents);
						st2.start();
						Thread.sleep(1000);
							}
						}
						
						//System.out.println("Could be this");
						}
					
						//System.out.println("FileName is "+m.fname);
						
						
						
				}
				//}this one
				
				else{
					String filename;
					int chunkIndex;
					int index=0;
					/*---Uncomment here--*/
					Thread.sleep(2000);
					boolean alreadyConnected=false;
					if(!Node.connectedNodes.isEmpty())
						for(int i=0;i<Node.connectedNodes.size();i++){
							if(Node.getNodeNumber(Node.connectedNodes.get(i))==Node.MServerNo)
							{
								alreadyConnected=true;
								//System.out.println("Append req- already connected");
								break;
							}
						}
					
						
						if(!alreadyConnected)
						 	while (true) {
								try {
									//clientSocket = new Socket("dc0" + Node.jFromFile[i] + ".utdallas.edu",Node.SERVER_PORT);
									
									clientSocket = new Socket(Node.MSERVER,Node.SERVER_PORT);
									if (clientSocket == null) 
									{
									Thread.sleep(1000);
									} else 
									{
									System.out.println("Client made connection with "+clientSocket.getInetAddress().getHostName());
									Node.connectedNodes.add(clientSocket);
									out = new ObjectOutputStream(clientSocket.getOutputStream());
									Node.outMap.put(Node.getNodeNumber(clientSocket),out);
									iis = new ObjectInputStream(clientSocket.getInputStream());
									ListeningThread lt = new ListeningThread(clientSocket,iis);
									lt.start();
									break;
									}
									} 
									catch (Exception e) {
									//e.printStackTrace();
									}
								}
					
					if(m.fname.contains("NN")){
					//if(m.fileContent.contains("new")){
						//System.out.println("At the right spot");
						chunkIndex = Integer.parseInt(Character.toString(m.fname.charAt(m.fname.length()-9)));
						//chunkIndex = Integer.parseInt(Character.toString(m.fname.charAt(6)));
						// filename= m.fname.substring(0, 6)+"c"+chunkIndex+".txt";
						 filename= m.fname.substring(0,m.fname.length()-2);
						// System.out.println("Filename is -"+filename);
						 
						 File file2 = new File(filename);
						 FileWriter fw = new FileWriter(filename,true);
						// System.out.println("Filename this time  -"+filename);
						 //System.out.println("Length of file -"+file2.length());
						 while(index<8192-file2.length())
						{
							 fw.write(" ");
							 index++;
						 }
						  //the true will append the new data
						 //System.out.println("chunkIndex is -"+filename);
						 //appends the string to the file
						    fw.close();
						  /*  --------Commented from here--------*/
						   /* chunkIndex++;
						   // filename= m.fname.substring(0, 6)+"c"+chunkIndex+".txt";
						    int serverTemp=Integer.parseInt(Node.currentNode.getHostName().substring(2,4));
						    filename = m.fname.substring(0,m.fname.length()-9)+chunkIndex+"_"+serverTemp+".txt";
						  // System.out.println("Filename this time is "+filename);
						    File file = new File(filename);
							file.createNewFile();
							FileWriter fw2 = new FileWriter(file.getAbsoluteFile());
							BufferedWriter bw = new BufferedWriter(fw2);
							bw.write(m.fileContent);
							bw.close();
							 File file3 = new File(filename);
								long filespace = file3.length();
								//System.out.println("Heartbeat msg sent ");
								//System.out.println("Filespace is " +filespace);
								String fileSize =String.valueOf(filespace);
								//System.out.println("Completed append to file "+filename);
								SendingThread st = new SendingThread(3,MessageType.HEARTBEAT,filename,fileSize);
								st.start();*/
						    
					}
					else if(m.fname.contains("N")){
						  // filename= m.fname.substring(0, 6)+"c"+chunkIndex+".txt";
						chunkIndex = Integer.parseInt(Character.toString(m.fname.charAt(m.fname.length()-8)));
						//System.out.println("chunk Index -"+chunkIndex);
					    int serverTemp=Integer.parseInt(Node.currentNode.getHostName().substring(2,4));
					   // System.out.println("Append appropriately routed!");
					    filename = m.fname.substring(0,m.fname.length()-8)+chunkIndex+"_"+serverTemp+".txt";
					 //  System.out.println("Filename this time is "+filename);
					    File file = new File(filename);
						file.createNewFile();
						FileWriter fw2 = new FileWriter(file.getAbsoluteFile());
						BufferedWriter bw = new BufferedWriter(fw2);
						bw.write(m.fileContent);
						bw.close();
						 File file3 = new File(filename);
							long filespace = file3.length();
							//System.out.println("Heartbeat msg sent ");
							//System.out.println("Filespace is " +filespace);
							String fileSize =String.valueOf(filespace);
							//System.out.println("Completed append to file "+filename);
							SendingThread st = new SendingThread(Node.MServerNo,MessageType.HEARTBEAT,filename,fileSize);
							st.start();
					}
					else{
						//System.out.println("Adding to existing chunk");
						//System.out.println("Append appropriately routed!");
						chunkIndex = Integer.parseInt(Character.toString(m.fname.charAt(m.fname.length()-7)));
						//chunkIndex =Integer.parseInt(Character.toString(m.fname.charAt(6)));
						//filename= m.fname.substring(0, 6)+"c"+chunkIndex+".txt";
						filename= m.fname;
						
						//System.out.println("Filename this time is "+filename);
						 FileWriter fw = new FileWriter(filename,true);
						 fw.write(m.fileContent);
						 fw.close();
						 File file2 = new File(filename);
							long filespace = file2.length();
							//System.out.println("Heartbeat msg sent");
							//System.out.println("Filespace is" +filespace);
							String info = String.valueOf(filespace);
							//String info = Node.currentNode.getHostName().charAt(3)+"_"+filespace;
							//System.out.println("Filespace is " +info);
							//System.out.println("Completed append to file when no new chunk gets created"+filename);
							SendingThread st = new SendingThread(Node.MServerNo,MessageType.HEARTBEAT,filename,info);
							st.start();
					}		
					
				    
				}
				}
				 else if(m.mt.equals(MessageType.READ))
					{
						int i=0;
						
						if(Node.currentNode.getHostName().equals(Node.MSERVER))
						{
							Thread.sleep(3000);
							int startIndex;int k=0;int offSet;String temp = "";ArrayList<Entry> temp3= new ArrayList<Entry>();
							int chunkI;
							//System.out.println("Reminder of string meant for read operation is "+m.fileContent);
							while(m.fileContent.charAt(i)!='|'){
							temp = temp+m.fileContent.charAt(i);
							i++;
							}
							//System.out.println("Value of start index is "+temp);
							startIndex=Integer.parseInt(temp);
							//System.out.println("Value of start index is "+startIndex);
							i++;
							//System.out.println("Value of offset is "+m.fileContent.substring(i));
							offSet = Integer.parseInt(m.fileContent.substring(i));
							//System.out.println("Value of offset is "+offSet);
							double tempvalue=(double) startIndex/8142.0;
						//	System.out.println("Double value is "+tempvalue);
							if(tempvalue<1) chunkI=1;
							else chunkI =(int)Math.ceil(tempvalue);
							//System.out.println("Chunk Index is "+Math.ceil(tempvalue));
							//System.out.println("Chunk Index is "+chunkI);
							int serverNo=4;int destServ = 4;boolean alreadySet=false;
							for(int q=0;q<Node.serverCount;q++)
							{
								if(Node.directory.get(serverNo)!=null &&!Node.directory.get(serverNo).isEmpty())
								{
									//	System.out.println("gets here");
										temp3=Node.directory.get(serverNo);
										for(k=0;k<temp3.size();k++) {
										//	System.out.println("gets here too");
											//System.out.println("Value 1 "+temp3.get(k).fname);
											//System.out.println("Value 2 "+m.fname.substring(0,m.fname.length()-4));
											//System.out.println("Value 3 "+chunkI);
											//System.out.println("Value 4 "+Integer.parseInt(Character.toString(temp3.get(k).fname.charAt(temp3.get(k).fname.length()-7))));
											if(temp3.get(k).fname.contains(m.fname.substring(0,m.fname.length()-4))&&
													chunkI==Integer.parseInt(Character.toString(temp3.get(k).fname.charAt(temp3.get(k).fname.length()-7)))){
												{	
													//System.out.println("gets here as well");
													destServ =serverNo;
													alreadySet=true;
													break;
													
												}
											}
										}
										
										
									
								}	//System.out.println("ChunkServer "+serverNo+" - "+Node.directory.get(serverNo).get(l));
								if(alreadySet)break;		
								serverNo++;
								
							}
							if(k<temp3.size())
							{
							String fname =destServ+ temp3.get(k).fname;
							//System.out.println("FileName is "+fname);
							String infoToSend = "R"+startIndex+"_"+offSet;
							//System.out.println("Start and offset are"+startIndex+" "+offSet);
							
							SendingThread st = new SendingThread(Node.getNodeNumber(socket),MessageType.MSERVRESP,fname,infoToSend);
							st.start();
							}
								
							
						}
						else{
							int index=1;String temp4="";int chunkIndex;
							//System.out.println("Character at 7th position " +m.fname.charAt(7));
							chunkIndex=Integer.parseInt(Character.toString(m.fname.charAt(m.fname.length()-7)));
							//System.out.println("Chunk index is "+chunkIndex);
							//File file = new File(m.fname.substring(0,m.fname.length()-5)); //subject to change
							File file = new File(m.fname); //subject to change
							//System.out.println("It gets here finally!");
							FileReader reader = new FileReader(file);
						       char[] chars = new char[(int) file.length()];
						       reader.read(chars);
						       while(m.fileContent.charAt(index)!='_'){
						    	   temp4 = temp4 +m.fileContent.charAt(index);
						    	   index++;
						       }
						       reader.close();
						       int startIndex=Integer.parseInt(temp4);
						       index++;
						    		   int offSet=Integer.parseInt(m.fileContent.substring(index));
						    		   if(chunkIndex>1){
						    			   startIndex=startIndex-(8192*(chunkIndex-1));
						    			  // System.out.println("New Start index is"+startIndex);
						    		   }
						    for(int r=0;r<offSet;r++)
						    {
						    	if(startIndex<chars.length)
						    	{System.out.print(chars[startIndex]);
						    	startIndex++;
						    	}
						    }
						}
					
					}
				else if(m.mt.equals(MessageType.HEARTBEAT))
				{
					
					ArrayList<Entry> fileChunksTemp = new ArrayList<Entry>();
					Entry entry=new  Entry();
					if(Node.currentNode.getHostName().equals(Node.MSERVER))
					{
						int serverNoTemp=Integer.parseInt(Character.toString(m.fname.charAt(m.fname.length()-5)));
						System.out.println("Heartbeat msg received from Server- "+serverNoTemp);
						if(Node.directory==null||Node.directory.size()==0||!Node.directory.containsKey(serverNoTemp)){
							 ArrayList<Entry> fileChunks = new ArrayList<Entry>();
							 
							 //System.out.println("New file chunk being added is - "+m.fname);
							 
							 //fileChunks.add(m.fname+"_"+m.fileContent.substring(2));
							 //System.out.println("File size is"+m.fileContent);
							 entry.fileSize = Integer.parseInt(m.fileContent);
							 entry.fname= m.fname;
							 fileChunks.add(entry);
							// System.out.println("Key is "+Integer.parseInt(Character.toString(m.fileContent.charAt(0))));
							 //System.out.println("Value is"+fileChunks.get(0));
							 Node.directory.put(serverNoTemp, fileChunks);
							 //System.out.println("Added contents !");
						 } 
						else{
							fileChunksTemp = Node.directory.get(serverNoTemp);
							Entry entry2=new  Entry();
							boolean isPresent=false;
							for(int y=0;y<fileChunksTemp.size();y++)
								{
								//System.out.println("Printed filename"+m.fname);
								//System.out.println("Does it get here?");
								//System.out.println("Value 1 - "+fileChunksTemp.get(y));
								//System.out.println("Value 2 - "+m.fname);
								if(fileChunksTemp.get(y).fname.equals((m.fname)))
								{
									//System.out.println("Does it get here?");
									//System.out.println("Value 1 - "+fileChunksTemp.get(y));
									//System.out.println("Value 2 - "+m.fname);	
									 
									 entry2.fileSize = Integer.parseInt(m.fileContent);
									 entry2.fname= m.fname;
									fileChunksTemp.remove(y);
									fileChunksTemp.add(entry2);
									//fileChunksTemp.add(m.fname+"_"+m.fileContent.substring(2));
									isPresent=true;
								}
								}
							
							//fileChunksTemp.add(m.fname+"_"+m.fileContent.substring(2));
							if(!isPresent){
								//Entry entry3=new  Entry();
								 entry2.fileSize = Integer.parseInt(m.fileContent);
								 entry2.fname= m.fname;
								
								fileChunksTemp.add(entry2);
								
							}
						//	System.out.println("New file chunk being added is - "+m.fname+"_"+m.fileContent.substring(2));
							//System.out.println("array size - "+fileChunksTemp.size());
							 //Node.directory.put(Integer.parseInt(Character.toString(m.fileContent.charAt(0))), fileChunksTemp);
							Node.directory.put(serverNoTemp, fileChunksTemp); 
							//System.out.println("Added contents!");
						}
						int serverNo=4;
						ArrayList<Entry> tempList = new ArrayList<Entry>();
						for(int k=0;k<Node.serverCount;k++){
							if(!Node.directory.isEmpty()&&Node.directory.containsKey(serverNo))
								tempList = Node.directory.get(serverNo);
								if(tempList!=null)
								for(int l=0;l<tempList.size();l++)
								//System.out.println("ChunkServer "+serverNo+" - "+tempList.get(l).fname+" - "+tempList.get(l).fileSize);
							serverNo++;
						}
						//Node.directory.put(Integer.parseInt(m.fileContent), m.fname);
						 // System.out.println("Hearbeat message received!");
					}	
				}
				
				else if(m.mt.equals(MessageType.MSERVRESP))
				{
					
					
					int serverNo;
					if(m.fileContent.contains("R"))
						serverNo = Integer.parseInt(Character.toString(m.fname.charAt(0)));
					else if(m.fname.contains("NN") &&!m.fileContent.contains("R"))
						serverNo = 	Integer.parseInt(Character.toString(m.fname.charAt(m.fname.length()-7)));
					else if(m.fname.contains("N") &&!m.fileContent.contains("R"))
						serverNo = 	Integer.parseInt(Character.toString(m.fname.charAt(m.fname.length()-6)));
					else 
						{serverNo = 	Integer.parseInt(Character.toString(m.fname.charAt(m.fname.length()-5)));
						//System.out.println("Server no at MServ Resp "+serverNo);
						}
					/*
				//	System.out.println("Filename at this point is "+m.fname);
					if(!m.fname.contains("NN")&&!m.fileContent.contains("R"))
						serverNo = 	Integer.parseInt(Character.toString(m.fname.charAt(m.fname.length()-5)));
					else if(m.fname.contains("NN") &&!m.fileContent.contains("R"))
						serverNo = 	Integer.parseInt(Character.toString(m.fname.charAt(m.fname.length()-7)));
					else
						{serverNo = Integer.parseInt(Character.toString(m.fname.charAt(0)));
					
							//System.out.println("File content is "+m.fileContent);
						}
							//System.out.println("Server Number is"+serverNo);
						*/
					
					boolean alreadyConnected=false;
					
						for(int i=0;i<Node.connectedNodes.size();i++){
							if(Node.getNodeNumber(Node.connectedNodes.get(i))==serverNo)
							{
								alreadyConnected=true;
								break;
							}
						}
						
						String ServerStr;
						if(serverNo<10)
							{ServerStr = "0"+String.valueOf(serverNo);
							//System.out.println("Server num is appropriately displayed - "+ServerStr);
							}
						else
							ServerStr = String.valueOf(serverNo);
						if(!alreadyConnected)
						 	while (true) {
								try {
									//clientSocket = new Socket("dc0" + Node.jFromFile[i] + ".utdallas.edu",Node.SERVER_PORT);
									
									clientSocket = new Socket("dc" + ServerStr + ".utdallas.edu",Node.SERVER_PORT);
									if (clientSocket == null) 
									{
									Thread.sleep(1000);
									} else 
									{
									System.out.println("Client made connection with "+clientSocket.getInetAddress().getHostName());
									Node.connectedNodes.add(clientSocket);
									out = new ObjectOutputStream(clientSocket.getOutputStream());
									Node.outMap.put(Node.getNodeNumber(clientSocket),out);
									iis = new ObjectInputStream(clientSocket.getInputStream());
									ListeningThread lt = new ListeningThread(clientSocket,iis);
									lt.start();
									break;
									}
									} 
									catch (Exception e) {
									//e.printStackTrace();
									}
								}
					if(!m.fileContent.contains("R")){
					SendingThread st = new SendingThread(serverNo,MessageType.APPEND,m.fname,m.fileContent);
					st.start();	
					}
					else{
						
						SendingThread st = new SendingThread(serverNo,MessageType.READ,m.fname.substring(1),m.fileContent);
						st.start();	
					}
				}
				else if(m.mt.equals(MessageType.SUCCESS)){
					System.out.println("Checking replica consistency:");
					if(!Node.otherFiles.isEmpty())
						Node.checkReplicaConsistency(Node.otherFiles);
					
				}
				
				
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				break;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Machine that went down is - "+NodeNo);
				int tempNode = -1;
				System.out.println("Size of connected nodes Before "+Node.connectedNodes.size());
				System.out.println("Size of directory nodes Before "+Node.directory.size());
				synchronized(Node.connectedNodes){
					for(int i=0;i<Node.connectedNodes.size();i++){
						if(NodeNo == Node.getNodeNumber(Node.connectedNodes.get(i)))
								tempNode= i;
					}
					synchronized(Node.directory){
						
							for(int index=0;index<Node.directory.get(NodeNo).size();index++)
							Node.otherFiles.add(Node.directory.get(NodeNo).get(index));
						Node.directory.remove(NodeNo);
						}
						
					}	
					Node.connectedNodes.remove(tempNode);
					Node.otherNodes.add(NodeNo);
					System.out.println("Size of connected nodes After "+Node.connectedNodes.size());
					System.out.println("Size of directory nodes After "+Node.directory.size());
					//Node.checkReplicaConsistency(fileList);
					break;
				} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
			
		}
	}
}
