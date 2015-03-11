import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Scanner;


public class Client extends Thread{
	Socket clientSocket;
	ObjectOutputStream out;
	ObjectInputStream iis;
	
	
	@Override
	public void run()
	{
		
	
		try 
		{
			
		    //File text = new File("input.txt");
			
			File text = new File(Node.fname);
		    String temp2=null;
		    String buffer =null;
		    boolean alreadyRead = false;
		    StringBuilder fullContent = new StringBuilder();
		    Scanner scr = new Scanner(text);
		    
			//Scanner scr = new Scanner(new File(Node.fname));
			//int test=0;
			char operation;String fileContent;
			String temp;
			//while(scr.hasNext()){
			
			while(scr.hasNext()||buffer!=null)
			{
				if(!alreadyRead)	
				 temp=scr.next();
				else
					{temp = buffer;
				buffer=null;}	
					operation =temp.charAt(0); 
				System.out.println("Operation to be performed is "+operation);
				if(temp.charAt(1)=='|') {
					int index=2;
					StringBuffer fileName=new StringBuffer("");
					while(index<temp.length()) {
							if(temp.charAt(index)=='|')
								break;
							fileName.append(temp.charAt(index));
						index++;
					}
				//	System.out.println("Filename is "+fileName);
					String fname = fileName.toString();
					while(scr.hasNext()){
					temp2 = scr.next();
					
					if(temp2.contains("|"))
						{
						buffer = temp2;
					//	System.out.println("buffer value is "+buffer);
						alreadyRead=true;
						break;
						}
					fullContent.append(" "+temp2);
					}
					if(!(operation=='r'))
					fileContent = 
						temp.substring(index+1) +fullContent.toString(); 
					else
						fileContent =temp.substring(index+1) ;
					SendingThread st;
					if(Node.connectedNodes.isEmpty())
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
					if(operation=='w') {
						
						Thread.sleep(5000);
						
						st = new SendingThread(Node.MServerNo,MessageType.WRITEREQ,fname,fileContent);
						st.start();
						fullContent=new StringBuilder();
					}
					
					else if(operation=='a'){
						Thread.sleep(5000);
						st = new SendingThread(Node.MServerNo,MessageType.APPEND,fname,fileContent);
						st.start();
						fullContent=new StringBuilder();
						
						
					}
					else if(operation=='r'){
						Thread.sleep(5000);
						st = new SendingThread(Node.MServerNo,MessageType.READ,fname,fileContent);
						st.start();
						fullContent=new StringBuilder();
						Thread.sleep(3000);
					}
					
				}
				
			}
				/*while(test<=2)
				{
				System.out.println("first string is "+scr.next());
			
				test++;
				}*/
			scr.close();
			Thread.sleep(10000);
			SendingThread st2 = new SendingThread(Node.MServerNo,MessageType.SUCCESS,"dummyStr","dummyStr");
			st2.start();
			while(true){
				
			Thread.sleep(30000);
			/*System.out.println("Closing client socket");
			clientSocket.close();*/
			break;
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		}

}
