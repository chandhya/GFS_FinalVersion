

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.io.ObjectOutputStream;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class Node {
	static InetSocketAddress currentNode;
	public static final int SERVER_PORT = 1930;
	public static String fname;
	public static int nodeNo;
	public static int initialServerNo=4;
	public static int serverCount=6;
	public static int replicaCount=3;
	public static int MServerNo=2;
	public static boolean[] selectedServerNos= new boolean[serverCount];
	public static String MSERVER="dc02.utdallas.edu";
	public static CopyOnWriteArrayList<Socket> connectedNodes = new CopyOnWriteArrayList<Socket>();
	public static CopyOnWriteArrayList<Entry> otherFiles  = new CopyOnWriteArrayList<Entry>();
	public static CopyOnWriteArrayList<Integer> otherNodes = new CopyOnWriteArrayList<Integer>();
	public static ConcurrentHashMap<Integer, Entry> serverLog = new ConcurrentHashMap<Integer,Entry>();
	public static ConcurrentHashMap<Integer, ObjectOutputStream> outMap = new ConcurrentHashMap<Integer, ObjectOutputStream>();
	public static ConcurrentHashMap<Integer, ArrayList<Entry>> directory = new ConcurrentHashMap<Integer, ArrayList<Entry>>();
	
	public static void main(String args[]){
		try {
			if(args.length>0)
				fname = args[0];
			currentNode = new InetSocketAddress(InetAddress.getLocalHost().getHostName(),
					SERVER_PORT);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		initializeNetwork();
	}
	
	private static void initializeNetwork() {
		try {
			
			currentNode = new InetSocketAddress(InetAddress.getLocalHost().getHostName(),
					SERVER_PORT);
			nodeNo = Integer.parseInt((currentNode.getHostName().substring(2,4)));
			
			
			Thread t1 = new Client();
			Thread t2 = new Server();
			Thread mServer = new MServer(); 
			if(fname!=null)
				t1.start();
			else if(currentNode.getHostName().equals(Node.MSERVER))
			{
				mServer.start();
				
				List<String> textFiles = new ArrayList<String>();
				textFiles =findTextFiles(System.getProperty("user.dir"));
				for(int i=0;i<textFiles.size();i++) {
					//System.out.println(textFiles.get(i));
					String textFile =textFiles.get(i);
					if(textFile.charAt(textFile.length()-6)=='_'&&textFile.charAt(textFile.length()-8)=='_'){
						Entry entry = new Entry();
						entry.fname =textFile;
						File file = new File(textFile);
						entry.fileSize=(int)file.length();
						
						//System.out.println("TextFileNow "+entry.fname+"_"+entry.fileSize);
						int serverNo = Integer.parseInt(Character.toString(textFile.charAt(textFile.length()-5)));
						ArrayList<Entry> entries = new ArrayList<Entry>();
						if(Node.directory.isEmpty()||!Node.directory.containsKey(serverNo)){
							entries.add(entry);
							Node.directory.put(serverNo, entries);
							//System.out.println("Filename, size, serverNo"+entry.fname+" - "+entry.fileSize+" - "+serverNo);
						}
							else if(Node.directory.containsKey(serverNo))
								{
								entries = Node.directory.get(serverNo);
								entries.add(entry);
								Node.directory.remove(serverNo);
								Node.directory.put(serverNo, entries);
								//System.out.println("Filename, size, serverNo"+entry.fname+" - "+entry.fileSize+" - "+serverNo);
								}
					}
				}int serverNo= initialServerNo;
				for(int j=0;j<serverCount;j++){
					List<Entry> entries2= new ArrayList<Entry>();
					if(Node.directory.containsKey(serverNo)){
					entries2=Node.directory.get(serverNo);
					System.out.println("Server No: "+serverNo);
					if(entries2!=null)
					for(int k=0;k<entries2.size();k++){
						System.out.println("filename is "+entries2.get(k).fname);
					}}
					serverNo++;
					
				}
				
			}
			else 
				t2.start();
			
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	//Method that selects servers based on totalspace and no of chunks in the server
	public static List<Integer> generateServerNumbers(){
		
		List<Entry> entries = new ArrayList<Entry>();
		Entry entry= new Entry();
		List<Integer> serverNos = new ArrayList<Integer>();
		int serverCount=0;
		if(!Node.directory.isEmpty())
			{System.out.println("directory not empty");
			System.out.println("Directory size now "+Node.directory.size());
			for(int serverNum:directory.keySet())
			{
				int totalspace = 0;int chunkCount=1;
				entries=directory.get(serverNum);
				System.out.println("Server No in context"+serverNum);
				if(entries!=null&&!entries.isEmpty()){
					for(int j=0;j<entries.size();j++){
						System.out.println("filename in context-"+entries.get(j));
						entry =entries.get(j);
						
						totalspace = totalspace+entry.fileSize;
						chunkCount =chunkCount+j;
						Entry temp = new Entry();
						
						temp.fileSize =totalspace;
						temp.chunkCount=chunkCount;
						serverLog.put(serverNum, temp);
					}
				}
				
			}}
		for(int serverNum:directory.keySet())
		{
			System.out.println("Entry is:");
			
			System.out.println(serverLog.get(serverNum).fileSize+" - "+serverLog.get(serverNum).chunkCount);
		}
		int chunkCount=0;int fileSpace=0;
		List<Integer> chunkCountList = new ArrayList<Integer>();
		List<Integer> fileSpaceList = new ArrayList<Integer>();
		for(int serverNum:serverLog.keySet())
		{
			Entry entry2 = new Entry();
			entry2=serverLog.get(serverNum);
			chunkCountList.add(entry2.chunkCount);
			fileSpaceList.add(entry2.fileSize);
			
		}
		Collections.sort(chunkCountList);
		System.out.println("ChunkCount0 - "+chunkCountList.get(0));
		System.out.println("ChunkCount1 - "+chunkCountList.get(chunkCountList.size()-1));
			if(chunkCountList.get(chunkCountList.size()-1)-chunkCountList.get(0)>=2)
			{
				int chunkCountTemp = chunkCountList.get(0);
				for(int serverNum:serverLog.keySet())
				{
					Entry entry2 = new Entry();
					entry2=serverLog.get(serverNum);
					if(entry2.chunkCount==chunkCountTemp)
					{
						serverNos.add(serverNum);
						System.out.println("Adding value in generate method " +serverNum);
						System.out.println("Chunk count is "+chunkCountTemp);
						serverCount=1;
					}
				}
			}
			System.out.println("Might be stuck here");
			Collections.sort(fileSpaceList);
			System.out.println("Filespace list "+fileSpaceList.size());
			int index = 0;boolean flagSet=false;
			while(true)
			{ 
				int currentSpace =fileSpaceList.get(index);
				for(int serverNum:serverLog.keySet())
				{
					
					
					Entry entry2 = new Entry();
					entry2=serverLog.get(serverNum);
					if(entry2.fileSize==currentSpace){
						index++;
						serverNos.add(serverNum);
						System.out.println("Adding value in generate method " +serverNum);
						serverCount++;
						if(serverCount==3)
							{flagSet=true;
							break;
							}
					}
				}
				if(flagSet)
					break;
			}
				
			System.out.println("Server Count is "+serverCount);
			for(int index2=0;index2<serverNos.size();index2++)
			System.out.println("Server Nos"+serverNos.get(index2));
		return serverNos;
		
	} 
	public static List<Integer> performInsertionSort(List<Integer> chunkCountList){
        
        int temp;
        
        for (int i = 1; i < chunkCountList.size(); i++) {
            for(int j = i ; j > 0 ; j--){
                if(chunkCountList.get(j) <= chunkCountList.get(j-1)){
                    temp = chunkCountList.get(j);
                    chunkCountList.add(j,  chunkCountList.get(j-1));
                    chunkCountList.add(j-1,  temp);
                }
            }
            
        }
        for(int k=0;k<chunkCountList.size();k++)
        {
        	System.out.println("Item in list - "+chunkCountList.get(k));
        }
        return chunkCountList;
    }
	
		 static List<String> findTextFiles(String directory) {
			  List<String> textFiles = new ArrayList<String>();
			  File dir = new File(directory);
			  for (File file : dir.listFiles()) {
			    if (file.getName().endsWith((".txt"))) {
			      textFiles.add(file.getName());
			    }
			  }
			  return textFiles;
			}
	    
	double power(long x, int n){
	    double pow =1L;
	    if(n==0)
	     return 1;
	     if(n==1)
	     return x;
	     if(n<0)
	     return 1.0/power(x,-n);
	     else 
	     {     
	     pow = power(x, n/2);
	     if(n%2==0)
	     return pow*pow;
	     else 
	     return pow*pow*x;
	      }
	}
	static double generateRandvalue(double vMin, double vMax) {

		double rand = Math.random();
		double range = vMax-vMin;
		double adjustment = range* rand;
		double result = vMin+adjustment;

		
		return result;
	
		
		
	}
	 static int getNodeNumber(Socket connectedNode) {
			if(connectedNode!=null)
			{
			int x = Integer.parseInt(Character.toString(connectedNode.getInetAddress().getHostName().charAt(3)));
			
			return x;
			}
			return 0;
		}


	public static void checkReplicaConsistency(List<Entry> fileList) {
		List<Integer> serverNos = new ArrayList<Integer>(); 
		List<Entry> filesToCopy = new ArrayList<Entry>();
		List<Integer> serverNosM = new ArrayList<Integer>();
		System.out.println("Directory Contents:");
		for(int serverI:Node.directory.keySet()){
			
			for(int u=0;u<Node.directory.get(serverI).size();u++)
				System.out.println("Server No: "+serverI+"- File: "+Node.directory.get(serverI).get(u));
		}
		
		
		for(int k=0;k<fileList.size();k++) {
			String fileName =fileList.get(k).fname.substring(0,fileList.get(k).fname.length()-8);
			System.out.println("CRC:FileName is "+fileName);
			boolean isPresent=false;
			int chunkindex =Integer.parseInt(Character.toString(fileList.get(k).fname.charAt(fileList.get(k).fname.length()-7)));
			System.out.println("CRC:ChunkIndex is "+chunkindex);
				for(int serverNo:Node.directory.keySet()) {
					
					List<Entry> entries = new ArrayList<Entry>(); 
					entries = Node.directory.get(serverNo);
					for(int y=0;y<entries.size();y++){
						String currentFile =entries.get(y).fname.substring(0,entries.get(y).fname.length()-8);
						System.out.println("CRC:FileName to compare is "+currentFile);
						int currChunkindex =Integer.parseInt(Character.toString(entries.get(y).fname.charAt(entries.get(y).fname.length()-7)));
						System.out.println("CRC:ChunkIndex to compare is "+currChunkindex);
						if(currentFile.equals(fileName) &&currChunkindex ==chunkindex)
						{
							if(!isPresent)
							filesToCopy.add(entries.get(y));
							System.out.println("CRC: GETS HERE!!!");
							isPresent =true;
							serverNosM.add(serverNo);
							break;
						}
						
					}
				//	if(isPresent)
					//break;
				}
		}
		for(int k=0;k<fileList.size();k++) {
			String fileName =fileList.get(k).fname.substring(0,fileList.get(k).fname.length()-8);
			System.out.println("CRC2:FileName is "+fileName);
			boolean isPresent=false;
			int chunkindex =Integer.parseInt(Character.toString(fileList.get(k).fname.charAt(fileList.get(k).fname.length()-7)));
			System.out.println("CRC2:ChunkIndex is "+chunkindex);
				for(int serverNo:Node.directory.keySet()){
					 isPresent=false;
					List<Entry> entries = new ArrayList<Entry>(); 
					entries = Node.directory.get(serverNo);
					System.out.println("CRC2:Server No is "+serverNo);
					for(int y=0;y<entries.size();y++){
						String currentFile =entries.get(y).fname.substring(0,entries.get(y).fname.length()-8);
						System.out.println("CRC2:FileName to compare is "+currentFile);
						int currChunkindex =Integer.parseInt(Character.toString(entries.get(y).fname.charAt(entries.get(y).fname.length()-7)));
						System.out.println("CRC2:ChunkIndex to compare is "+currChunkindex);
						if(currentFile.equals(fileName) &&currChunkindex ==chunkindex)
						{
							System.out.println("CRC2: GETS HERE !!!");
							isPresent =true;
							//filesToCopy.add(entries.get(y));
							break;
						}
						
					}
					if(!isPresent){
						if(!serverNos.contains(serverNo))
						{serverNos.add(serverNo);
						break;
						}
						else
							continue;
					}
				}
		}
		System.out.println("FilesToCopyFrom "+filesToCopy.size());
		System.out.println("serverNos "+serverNos.size());
		if(serverNos.size()==0 &&filesToCopy.size()>0){
			for(int serverKey:Node.directory.keySet()){
				if(!serverNosM.contains(serverKey)){
					serverNos.add(serverKey);
				}
			}
			
		}
		
		for(int h=0;h<fileList.size();h++){
			if(filesToCopy.size()>0 &&serverNos.size()>0)
			if(h<filesToCopy.size() && h<serverNos.size()){
				System.out.println("File chunks Transfer from Crashed Nodes:");
				System.out.println("FileChunk - "+fileList.get(h)+" transferred to Node "+serverNos.get(h));
				//System.out.println("CRC:File item - "+fileList.get(h)+"to copy from - "+filesToCopy.get(h)+" S No- "+serverNos.get(h));
		}
		}
		
		for(int h=0;h<fileList.size();h++){
			String fileToCopyFrom = null;
			if(h<filesToCopy.size() && h<serverNos.size())
				fileToCopyFrom =filesToCopy.get(h).fname;
			File file = new File(fileToCopyFrom); //subject to change
			//System.out.println("It gets here finally!");
			FileReader reader;
			try {
				reader = new FileReader(file);
				char[] chars = new char[(int) file.length()];
			      reader.read(chars);
			   String newFile = fileToCopyFrom.substring(0,fileToCopyFrom.length()-5)+serverNos.get(h)+".txt";
			   File file2 = new File(newFile);
			   file2.createNewFile();
			   FileWriter fw = new FileWriter(file2.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw);
				
				bw.write(chars);
				bw.close();
				File file3 = new File(fileList.get(h).fname);
				file3.delete();//subject to change	
			       
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		       
		}
	}
	
}
