
/*@author chandhya
 * The serialize message class has all fields required for message transfer
 * 
 * */
import java.io.Serializable;
import java.net.InetSocketAddress;





public class Message implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String fname; // Physical clock
	String fileContent;
	MessageType mt; 
	char operation;
	public Message(String fname,  
			MessageType mt, String fileContent) {
		
		this.fname = fname;
		this.fileContent =fileContent;
		this.mt = mt;
		
	}

}
