import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;


public class MServer extends Thread{
	Socket socket;
	Socket clientSocket;
    ObjectInputStream iis;
    ObjectInputStream iis2;
    ObjectOutputStream out;
    ObjectOutputStream out2;
    int index=1;
	@Override
	public void run() {
		ServerSocket serverSocket=null;
		try {
			serverSocket = new ServerSocket(Node.SERVER_PORT);
			while(true){
				socket = serverSocket.accept();
				System.out.println("MServer made connection with Machine "+Node.getNodeNumber(socket));
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
