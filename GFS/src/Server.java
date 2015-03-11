import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;


public class Server extends Thread {
	
	Socket socket;
    ObjectInputStream iis;
    ObjectOutputStream out;
    
	@Override
	public void run() {
		ServerSocket serverSocket=null;
		try {
			serverSocket = new ServerSocket(Node.SERVER_PORT);
		while(true){	
				System.out.println("Waiting for connection");
				socket = serverSocket.accept();
				System.out.println("Machine I'm connected to is "+Node.getNodeNumber(socket));
				Node.connectedNodes.add(socket);
				out = new ObjectOutputStream(socket.getOutputStream());
				Node.outMap.put(Node.getNodeNumber(socket),out);
				iis = new ObjectInputStream(socket.getInputStream());
				ListeningThread lt = new ListeningThread(socket,iis);
				lt.start();
		
		}
				
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			
		}
       
}
}