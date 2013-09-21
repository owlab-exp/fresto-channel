
import org.apache.thrift.TSerializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import fresto.command.CommandEvent;

public class SubmitCommand {

	public static void main(String[] args) {
		if(args.length < 2) {
			System.out.println("Argements: <target module> <command>");
			//throw new IllegalArgumentException("Valid arguments: <module name> <command>");
			System.exit(0);
		}
		TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket publisher = context.socket(ZMQ.PUB);
		publisher.connect("tcp://fresto1.owlab.com:7000");
		publisher.connect("tcp://fresto1.owlab.com:7002");
		String commandTopic = "CMD";
		CommandEvent event = new CommandEvent();
		event.target_module = args[0];
		event.command = args[1];
		try {
			System.out.println("Sending command...");
			byte[] eventBytes = serializer.serialize(event);
			publisher.send(commandTopic.getBytes(), ZMQ.SNDMORE);
			publisher.send(eventBytes, 0);
			System.out.println(args[0] + " " + args[1] + " sent.");
		} catch(TException te) {
			te.printStackTrace();
		}
		//Thread.sleep(1000);
		publisher.close();
		context.term();
	}
}
