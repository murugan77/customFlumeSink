package soma.flume.customSink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;


public class CustomSocketSink extends AbstractSink implements Configurable{

	private Context context;
	private int port;
	private String host;
	private Socket socket;
	private BufferedWriter bufferedWriter;

	@Override
	public synchronized void stop() {
		// TODO Auto-generated method stub
		super.stop();
		try {
			this.bufferedWriter.close();
			this.socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void configure(Context context) {
		 this.context = context;
		    this.host = context.getString("host");
		    this.port = Integer.parseInt(context.getString("port"));
			
			try {
				this.socket = new Socket(this.host, this.port);
				this.bufferedWriter = new BufferedWriter(
						new OutputStreamWriter(socket.getOutputStream()));
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}

	@Override
	public Status process() throws EventDeliveryException {
	    Status result = Status.READY;
	    Channel channel = getChannel();
	    Transaction transaction = channel.getTransaction();
	    Event event = null;

	    try {
	      transaction.begin();
	      event = channel.take();

	      if (event != null) {
	    	  String message = new String(event.getBody())+"\n";
	    	  System.out.println("Event: " + message);
	    	  bufferedWriter.write(message, 0, message.length());
	    	  bufferedWriter.flush();	    	
	      } else {
	        // No event found, request back-off semantics from the sink runner
	        result = Status.BACKOFF;
	      }
	      transaction.commit();
	    } catch (Exception ex) {
	      transaction.rollback();
	      throw new EventDeliveryException("Failed to log event: " + event, ex);
	    } finally {
	      transaction.close();
	    }

	    return result;
	}
	
	
	
}