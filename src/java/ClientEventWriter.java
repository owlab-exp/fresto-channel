import java.util.Iterator;
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

//import fresto.pail.SplitFrestoDataPailStructure;

import fresto.command.CommandEvent;

public class ClientEventWriter {
	private static String THIS_CLASS_NAME = "ClientEventWriter";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);

	private static final String HDFS_PATH = "hdfs://fresto1.owlab.com:9000/fresto/new";
	private static final String ZMQ_URL = "tcp://fresto1.owlab.com:7004";
	private static final String TOPIC_REQUEST = "CB";
	private static final String TOPIC_RESPONSE = "CF";
	private static final String TOPIC_COMMAND_EVENT = "CMD";
	
	private static TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
	
	private static final SplitFrestoDataPailStructure pailStructure = new SplitFrestoDataPailStructure();
	private Pail<FrestoData> pail;
	private TypedRecordOutputStream tros;

	private static boolean work = true;
	private static int SLEEP_TIME = 100;

	public static void main(String[] args) throws Exception {

		ClientEventWriter eventWriter = new ClientEventWriter();

		eventWriter.createPail();

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket puller = context.socket(ZMQ.PULL);
		puller.connect(ZMQ_URL);

		//Consume socket data
		FrestoEventQueue frestoEventQueue = new FrestoEventQueue(puller);
		frestoEventQueue.start();

		while(work) {

			// To add sufficient events to the queue
			Thread.sleep(SLEEP_TIME);

			int queueSize = frestoEventQueue.size();
			
			if(queueSize > 0) {
				eventWriter.openRecordStream();

				for(int i = 0; i < queueSize; i++) {
					FrestoEvent frestoEvent = frestoEventQueue.poll(); 
					if(TOPIC_COMMAND_EVENT.equals(frestoEvent.topic)) {
						eventWriter.handleCommand(frestoEvent.topic, frestoEvent.eventBytes);
						continue;
					}
					eventWriter.writePailData(frestoEvent.topic, frestoEvent.eventBytes);
				}

				eventWriter.closeRecordStream();
				LOGGER.info(queueSize + " events processed.");
			} else {
				LOGGER.info(queueSize + " events.");

			}

		}

		puller.close();
		context.term();
	}

	public void handleCommand(String topic, byte[] eventBytes) throws TException {
		LOGGER.info("A command received."); 
		CommandEvent event = new CommandEvent(); 
		deserializer.deserialize(event, eventBytes); 
		if(event.target_module.equalsIgnoreCase(THIS_CLASS_NAME)) { 
			if(event.command.equalsIgnoreCase("exit")) { 
				LOGGER.info("Perform command: " + event.command); 
				work = false; 
			} else { 
				LOGGER.warning("Unsupported command: " + event.command); 
			} 
		}
	}


	public void createNewPail() {
		try {
			pail = Pail.create(HDFS_PATH, pailStructure);
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	public void createPail() {
		try {
			if(pail == null) {
				pail = new Pail<FrestoData>(HDFS_PATH);
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

	public void openRecordStream() throws IOException {
		tros = pail.openWrite();
	}

	public void closeRecordStream() throws IOException {
		tros.close();
	}

	public void writePailData(String topic, byte[] eventBytes) throws TException, IOException {
	//	tros = pail.openWrite();
		if(TOPIC_REQUEST.equals(topic) || TOPIC_RESPONSE.equals(topic)) {

			FrestoData frestoData = new FrestoData();
			deserializer.deserialize(frestoData, eventBytes);
			tros.writeObject(frestoData);
		} else {
			LOGGER.warning("Event topic: " + topic + " not recognized. Possible valures: " + TOPIC_REQUEST + " or " + TOPIC_RESPONSE); 
		}
	//	tros.close();
	}
}

