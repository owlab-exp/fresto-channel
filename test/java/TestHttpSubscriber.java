
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import fresto.event.HttpRequestEvent;

public class TestHttpSubscriber {

	public static void main(String[] args) {
		TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
		subscriber.connect("tcp://fresto1.owlab.com:7003");
		subscriber.subscribe("".getBytes());

		while(true) {
			System.out.println("Waiting...");
			String envelope = new String(subscriber.recv(0));
			byte[] messageBytes = subscriber.recv(0);
			try {
				HttpRequestEvent event = new HttpRequestEvent();
				deserializer.deserialize(event, messageBytes);
				System.out.println("Message Envelope: " + envelope);
			       System.out.println("Event.httpMethod : " + event.getHttpMethod());
			       System.out.println("Event.localHost : " + event.getLocalHost());
			       System.out.println("Event.localPort : " + event.getLocalPort());
			       System.out.println("Event.contextPath : " + event.getContextPath());
			       System.out.println("Event.servletPath : " + event.getServletPath());
			       System.out.println("Event.frestoUuid : " + event.getFrestoUUID());
			       System.out.println("Event.timestamp : " + event.getTimestamp());
			} catch(TException te) {
				te.printStackTrace();
			}
		}
	}
}

