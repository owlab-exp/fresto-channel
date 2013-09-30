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

import fresto.data.FrestoData;
import fresto.data.DataUnit;
import fresto.data.Pedigree;

import fresto.command.CommandEvent;

public class APEventWriter {
	private static String THIS_CLASS_NAME = "APEventWriter";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);
	private static final String ZMQ_URL = "tcp://fresto1.owlab.com:7005";
	private static final String TOPIC_ENTRY_CALL = "EB";
	private static final String TOPIC_ENTRY_RETURN = "EF";
	private static final String TOPIC_COMMAND_EVENT = "CMD";
	private static final String HDFS_PATH = "hdfs://fresto1.owlab.com:9000/fresto/new";
	private static final SplitFrestoDataPailStructure pailStructure = new SplitFrestoDataPailStructure();
	private TypedRecordOutputStream tros;
	private static TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());

	public static void main(String[] args) throws Exception {
		APEventWriter eventWriter = new APEventWriter();

		ZMQ.Context context = ZMQ.context(1);
		ZMQ.Socket puller = context.socket(ZMQ.PULL);
		puller.connect(ZMQ_URL);

		//puller.subscribe(TOPIC_ENTRY_CALL.getBytes());
		//puller.subscribe(TOPIC_ENTRY_RETURN.getBytes());
		//puller.subscribe(TOPIC_COMMAND_EVENT.getBytes());

		while(true) {
		//for(int i = 0; i < 10; i++){
			LOGGER.info("Waiting...");
			String topic = new String(puller.recv(0));
			
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
			eventWriter.openPail();
			eventWriter.appendPailData(topic, eventBytes, System.currentTimeMillis());
			eventWriter.closePail();
			LOGGER.info("Time taken: " + (System.currentTimeMillis() - startTime) + " ms");	
		}

		puller.close();
		context.term();
	}

	public void createPail() {
		try {
			if(tros == null) {
				Pail<FrestoData> pail = Pail.create(HDFS_PATH, pailStructure);
				tros = pail.openWrite();
			}
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}

	public void openPail() {
		try {
			if(tros == null) {
				Pail<FrestoData> pail = new Pail<FrestoData>(HDFS_PATH);
				tros = pail.openWrite();
			}
		} catch(IOException e){
			//throw new RuntimeException(e);
			//e.printStackTrace();
			LOGGER.info("Pail open failed, trying to create pail.");
			createPail();
		} catch(IllegalArgumentException e){
			//throw new RuntimeException(e);
			//e.printStackTrace();
			LOGGER.info("Pail open failed, trying to create pail.");
			createPail();
		}
	}

	public void closePail() {
		try {
			if(tros != null) 
				tros.close();
			tros = null;
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void appendPailData(String topic, byte[] eventBytes, long timestamp) throws TException, IOException {
		if(TOPIC_ENTRY_CALL.equals(topic) || TOPIC_ENTRY_RETURN.equals(topic)) {
			FrestoData frestoData = new FrestoData();
			deserializer.deserialize(frestoData, eventBytes);
			
			Pedigree pedigree = new Pedigree();
			pedigree.setReceivedTime(timestamp);

			frestoData.setPedigree(pedigree);

			
			tros.writeObject(frestoData);

		} else {
			LOGGER.info("Topic not recognized: " + topic);
		}
	}
}

