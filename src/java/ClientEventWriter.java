import java.util.logging.Logger;
import java.io.IOException;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailStructure;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

import fresto.event.HttpRequestEvent;
import fresto.event.HttpResponseEvent;

import fresto.data.FrestoData;

import fresto.command.CommandEvent;

public class ClientEventWriter {
	private static String THIS_CLASS_NAME = "ClientEventWriter";
	private static final String HDFS_URL = "hdfs://fresto1.owlab.com:9000/fresto/new";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);
	private static final String ZMQ_URL = "tcp://fresto1.owlab.com:7004";
	private static final String TOPIC_REQUEST = "CB";
	private static final String TOPIC_RESPONSE = "CF";
	private static final String TOPIC_COMMAND_EVENT = "CMD";
	private static TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
	private static final SplitFrestoDataPailStructure pailStructure = new SplitFrestoDataPailStructure();
	private Pail<FrestoData> pail;
	private TypedRecordOutputStream tros;

	public static void main(String[] args) throws Exception {

		ClientEventWriter eventWriter = new ClientEventWriter();

		eventWriter.createPail();

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket puller = context.socket(ZMQ.PULL);
		puller.connect(ZMQ_URL);

		while(true) {
		//for(int i = 0; i < 10; i++){
			LOGGER.info("Waiting...");
			String topic = new String(puller.recv(0));
			//LOGGER.info("Topic=" + topic + " and matching=" + TOPIC_REQUEST.equals(topic));

			byte[] eventBytes = puller.recv(0);
			LOGGER.info(eventBytes.length + " bytes received");

			if(TOPIC_COMMAND_EVENT.equals(topic)) {
				LOGGER.info("A command received.");
				CommandEvent event = new CommandEvent();
				deserializer.deserialize(event, eventBytes);
				if(event.target_module.equalsIgnoreCase(THIS_CLASS_NAME)) {
					if(event.command.equalsIgnoreCase("exit")) {
						LOGGER.info("Perform command: " + event.command);
						break;
					} else {
						LOGGER.warning("Unsupported command: " + event.command);
						LOGGER.warning("Supported: exit");
					}
				}
			}

			// Append Pail Data
			long startTime = System.currentTimeMillis();

			eventWriter.writePailData(topic, eventBytes);

			LOGGER.info("Time taken for writing: " + (System.currentTimeMillis() - startTime) + " ms");
		}

		puller.close();
		context.term();
	}

	public void createNewPail() {
		try {
			pail = Pail.create(HDFS_URL, pailStructure);
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	public void createPail() {
		try {
			if(pail == null) {
				pail = new Pail<FrestoData>(HDFS_URL);
			}
		} catch(IOException e){
			//throw new RuntimeException(e);
			//e.printStackTrace();
		} catch(IllegalArgumentException e){
			//throw new RuntimeException(e);
			//e.printStackTrace();
			LOGGER.info("Pail object new failed, trying to create a pail structure.");
			createNewPail();
		}
	}

	public void writePailData(String topic, byte[] eventBytes) throws TException, IOException {
		tros = pail.openWrite();
		if(TOPIC_REQUEST.equals(topic) || TOPIC_RESPONSE.equals(topic)) {

			FrestoData frestoData = new FrestoData();
			deserializer.deserialize(frestoData, eventBytes);
			tros.writeObject(frestoData);
		} else {
			LOGGER.warning("Event topic: " + topic + " not recognized. Possible valures: " + TOPIC_REQUEST + " or " + TOPIC_RESPONSE); 
		}
		tros.close();
	}
}

