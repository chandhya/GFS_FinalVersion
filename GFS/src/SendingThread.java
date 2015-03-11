import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;


public class SendingThread extends Thread implements Runnable{
	
	int receiver;
	MessageType mt;
	ObjectOutputStream oos;
	Message m1;
	String fname;
	String fileContent;
	SendingThread(int receiver, MessageType mt,String fileName, String fileContent)
	{
		this.receiver =receiver;
		this.fname=fileName;
		this.fileContent = fileContent;
		this.mt=mt;
	}
	
	@Override
	public void run(){
		if(mt.equals(MessageType.WRITEREQ))
		{
			if(receiver==Node.MServerNo)
			{
			try {
			
				 m1 = new Message(fname,MessageType.WRITEREQ,fileContent);
				 oos = Node.outMap.get(Node.MServerNo);
				 System.out.println("Sending write request to MServer"); 
				 oos.writeObject(m1);
				 oos.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}
			else{
				int chunkIndex = 1;
				int serverNo = receiver;
				System.out.println("filename is"+fname);
				String fileChunkName = fname.substring(0, fname.length()-4)+"_"+chunkIndex+"_"+serverNo+".txt";
				System.out.println("Filename is "+fileChunkName);
				m1 = new Message(fileChunkName,MessageType.WRITEREQ,fileContent);
				//double cServerNo = Node.generateRandvalue(4, 7);
				 oos = Node.outMap.get(receiver);
				 System.out.println("Sending write req to cServerNo: "+receiver); 
				 try {
					oos.writeObject(m1);
					oos.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 
			}
		}
		else if(mt.equals(MessageType.APPEND)) {
			if(receiver==Node.MServerNo)
			{
				try {
					 m1 = new Message(fname,MessageType.APPEND,fileContent);
					 oos = Node.outMap.get(Node.MServerNo);
					// System.out.println("Sending append req to MServer");
					oos.writeObject(m1);
					oos.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 
			}
			else {
				m1 = new Message(fname,MessageType.APPEND,fileContent);
				 oos = Node.outMap.get(receiver);
				 System.out.println("Sending append req to chunk Server");
				try {
					oos.writeObject(m1);
					oos.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
		}
		else if(mt.equals(MessageType.HEARTBEAT)) {
			try {
				 m1 = new Message(fname,MessageType.HEARTBEAT,fileContent);
				 oos = Node.outMap.get(Node.MServerNo);
				 System.out.println("Sending heartbeat message to MServer");
				oos.writeObject(m1);
				oos.flush();
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			
		}
		else if(mt.equals(MessageType.MSERVRESP)) {
			 m1 = new Message(fname,MessageType.MSERVRESP,fileContent);
			 oos = Node.outMap.get(receiver);
			 System.out.println("Sending metadata to client back from MServer");
			 try {
				oos.writeObject(m1);
				oos.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
		}
		else if(mt.equals(MessageType.READ)) {
			 m1 = new Message(fname,MessageType.READ,fileContent);
			 oos = Node.outMap.get(receiver);
			 System.out.println("Sending read req to MServer/Chunk Server");
			 try {
				oos.writeObject(m1);
				oos.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if(mt.equals(MessageType.SUCCESS)){
			 m1 = new Message(fname,MessageType.SUCCESS,fileContent);
			 oos = Node.outMap.get(receiver);
			 System.out.println("Sending final msg to MServer");
			 try {
				oos.writeObject(m1);
				oos.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
