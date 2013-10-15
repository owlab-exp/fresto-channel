import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.logging.Logger;
import java.io.IOException;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import fresto.data.FrestoData;
import fresto.data.Pedigree;
import fresto.data.DataUnit;
import fresto.data.ClientID;
import fresto.data.ResourceID;
import fresto.data.OperationID;
import fresto.data.RequestEdge;
import fresto.data.ResponseEdge;
import fresto.data.EntryOperationCallEdge;
import fresto.data.EntryOperationReturnEdge;
import fresto.data.OperationCallEdge;
import fresto.data.OperationReturnEdge;
import fresto.command.CommandEvent;

//import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
//import com.tinkerpop.blueprints.impls.orient.OrientGraph;
//import com.tinkerpop.blueprints.Graph;
//import com.tinkerpop.blueprints.Vertex;
//import com.tinkerpop.blueprints.Edge;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.OCommandSQL;

public class OrientEventWriter {
	private static String THIS_CLASS_NAME = "OrientEventWriter";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);

	private static final String ZMQ_URL = "tcp://fresto1.owlab.com:7002";

	private static final String DB_URL = "remote:fresto3.owlab.com/frestodb";
	private static final String DB_USER = "admin";
	private static final String DB_PASSWORD = "admin";

	// Client Events
	private static final String TOPIC_REQUEST = "CB";
	private static final String TOPIC_RESPONSE = "CF";

	// Server Events
	private static final String TOPIC_ENTRY_CALL = "EB";
	private static final String TOPIC_ENTRY_RETURN = "EF";
	private static final String TOPIC_OPERATION_CALL = "OB";
	private static final String TOPIC_OPERATION_RETURN = "OF";

	// Command Events
	private static final String TOPIC_COMMAND_EVENT = "CMD";
	
	//private FrestoData frestoData = new FrestoData();
	private static TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
	

	private static boolean work = true;
	private static int SLEEP_TIME = 10;

	private static OGraphDatabase oGraph;

	public OrientEventWriter() {
		this.oGraph = openDatabase();

	}

	public static void main(String[] args) throws Exception {

		OrientEventWriter eventWriter = new OrientEventWriter();


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
				for(int i = 0; i < queueSize; i++) {
					FrestoEvent frestoEvent = frestoEventQueue.poll(); 
					if(TOPIC_COMMAND_EVENT.equals(frestoEvent.topic)) {
						eventWriter.handleCommand(frestoEvent.topic, frestoEvent.eventBytes);
						continue;
					}
					eventWriter.writeEventData(frestoEvent.topic, frestoEvent.eventBytes);
				}
				LOGGER.info(queueSize + " events processed.");
			} else {
				LOGGER.info(queueSize + " events.");

			}

		}

		oGraph.close();

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

	public OGraphDatabase openDatabase() {
		OGraphDatabase oGraph = new OGraphDatabase(DB_URL);
		oGraph.open(DB_USER, DB_PASSWORD);

		return oGraph;
	}

	public void writeEventData(String topic, byte[] eventBytes) throws TException, IOException {
		if(TOPIC_REQUEST.equals(topic) 
			|| TOPIC_RESPONSE.equals(topic)
			|| TOPIC_ENTRY_CALL.equals(topic)
			|| TOPIC_ENTRY_RETURN.equals(topic)
			|| TOPIC_OPERATION_CALL.equals(topic)
			|| TOPIC_OPERATION_RETURN.equals(topic)
			) {

			FrestoData frestoData = new FrestoData();
			//Reuse
			//frestoData.clear();
			deserializer.deserialize(frestoData, eventBytes);

			Pedigree pedigree = new Pedigree();
                        pedigree.setReceivedTime(System.currentTimeMillis());

                        frestoData.setPedigree(pedigree);

			if(frestoData.dataUnit.isSetRequestEdge()) {
				RequestEdge requestEdge = frestoData.dataUnit.getRequestEdge();
				ClientID clientId = requestEdge.clientId;
				ResourceID resourceId = requestEdge.resourceId;
				//requestEdge.referrer;
				//requestEdge.method;
				//requestEdge.timestamp;
				//requestEdge.uuid;

				OSQLSynchQuery<ODocument> oQuery = new OSQLSynchQuery<ODocument>();
				Map<String, Object> params = new HashMap<String, Object>();

				oQuery.setText("select from Client where ip = :ip");
				params.put("ip", clientId.getClientIp());

				//ODocument clientV = findOne(oGraph, oQuery, params);
				ODocument clientV = findOne(oGraph, oQuery, params);
				if(clientV == null) {
					clientV = oGraph.createVertex("Client")
						.field("ip", clientId.getClientIp());
				}

				oQuery.setText("select from Resource where url = :url");
				params.clear();
				params.put("url", resourceId.getUrl());

				ODocument resourceV = findOne(oGraph, oQuery, params);

				if(resourceV == null) {
					resourceV = oGraph.createVertex("Resource")
						.field("url", resourceId.getUrl());
				}

				ODocument requestE = oGraph.createEdge(clientV, resourceV, "RequestEdge")
					.field("referrer", requestEdge.referrer)
					.field("method", requestEdge.method)
					.field("timestamp", requestEdge.timestamp)
					.field("uuid", requestEdge.uuid);

				requestE.save();

				linkToTS(oGraph, requestE.getIdentity(), "request", requestEdge.timestamp);
				
				oGraph.commit();

			} else if(frestoData.dataUnit.isSetResponseEdge()) {
				ResponseEdge responseEdge = frestoData.dataUnit.getResponseEdge();
				ClientID clientId = responseEdge.clientId;
				ResourceID resourceId = responseEdge.resourceId;
				
				// find or make client vertex
				OSQLSynchQuery<ODocument> oQuery = new OSQLSynchQuery<ODocument>();
				oQuery.setText("select from Client where ip = :ip");
				Map<String, Object> params = new HashMap<String, Object>();
				params.put("ip", clientId.getClientIp());

				ODocument clientV = findOne(oGraph, oQuery, params);
				if(clientV == null) {
					clientV = oGraph.createVertex("Client")
						.field("ip", clientId.getClientIp());
				}

				// find or make resource vertex
				oQuery.setText("select from Resource where url = :url");
				params.clear();
				params.put("url", resourceId.getUrl());

				ODocument resourceV = findOne(oGraph, oQuery, params);

				if(resourceV == null) {
					resourceV = oGraph.createVertex("Resource")
						.field("url", resourceId.getUrl());
				}

				ODocument responseE = oGraph.createEdge(resourceV, clientV, "ResponseEdge")
					.field("httpStatus", responseEdge.httpStatus)
					.field("elapsedTime", responseEdge.elapsedTime)
					.field("timestamp", responseEdge.timestamp)
					.field("uuid", responseEdge.uuid);

				responseE.save();

				linkToTS(oGraph, responseE.getIdentity(), "response", responseEdge.timestamp);
				//responseE.setProperties(props);
				oGraph.commit();

			} else if(frestoData.dataUnit.isSetEntryOperationCallEdge()) {
				EntryOperationCallEdge entryOperationCallEdge = frestoData.dataUnit.getEntryOperationCallEdge();
				ResourceID resourceId = entryOperationCallEdge.resourceId;
				OperationID operationId = entryOperationCallEdge.OperationId;

				OSQLSynchQuery<ODocument> oQuery = new OSQLSynchQuery<ODocument>();
				oQuery.setText("select from Host where hostName = :hostName");
				Map<String, Object> params = new HashMap<String, Object>();
				params.put("hostName", entryOperationCallEdge.localHost);
				ODocument hostV = findOne(oGraph, oQuery, params);

				if(hostV == null) {
					hostV = oGraph.createVertex("Host")
						.field("hostName", entryOperationCallEdge.localHost);
				}

				oQuery.setText("select from WebApplication where contextPath = :contextPath and port = :port");
				params.clear();
				params.put("contextPath", entryOperationCallEdge.contextPath);
				params.put("port", entryOperationCallEdge.localPort);
				ODocument webApplicationV = findOne(oGraph, oQuery, params);

				if(webApplicationV == null) {
					webApplicationV = oGraph.createVertex("WebApplication")
						.field("contextPath", entryOperationCallEdge.contextPath)
						.field("port", entryOperationCallEdge.localPort);
				}

				Set<OIdentifiable> edgeSet0 = oGraph.getEdgesBetweenVertexes(hostV, webApplicationV);
				if(edgeSet0.size() == 0) {
					ODocument serveApplicationE = oGraph.createEdge(hostV, webApplicationV, "ServeApplicationEdge");
					serveApplicationE.save();
				}
				
				oQuery.setText("select from ManagedResource where servletPath = :servletPath");
				params.clear();
				params.put("servletPath", entryOperationCallEdge.servletPath);
				ODocument managedResourceV = findOne(oGraph, oQuery, params);

				if(managedResourceV == null) {
					managedResourceV = oGraph.createVertex("ManagedResource")
						.field("servletPath", entryOperationCallEdge.servletPath);
				}

				Set<OIdentifiable> edgeSet1 = oGraph.getEdgesBetweenVertexes(webApplicationV, managedResourceV); 
				if(edgeSet1.size() == 0) { 
					ODocument manageResourceE = oGraph.createEdge(webApplicationV, managedResourceV, "ManageResourceEdge"); 
					manageResourceE.save();

				}

				oQuery.setText("select from Operation where operationName = :operationName and typeName = :typeName");
				params.clear();
				params.put("operationName", operationId.getOperationName());
				params.put("typeName", operationId.getTypeName());
				ODocument operationV = findOne(oGraph, oQuery, params);

				if(operationV == null) {
					operationV = oGraph.createVertex("Operation")
						.field("operationName", operationId.getOperationName())
						.field("typeName", operationId.getTypeName());
				}

				ODocument entryOperationCallE = oGraph.createEdge(managedResourceV, operationV, "EntryOperationCallEdge")
					.field("httpMethod", entryOperationCallEdge.httpMethod)
					.field("timestamp", entryOperationCallEdge.timestamp)
					.field("uuid", entryOperationCallEdge.uuid)
					.field("sequence", entryOperationCallEdge.sequence);

				entryOperationCallE.save();

				linkToTS(oGraph, entryOperationCallE.getIdentity(), "entryCall", entryOperationCallEdge.timestamp);

				oGraph.commit();


			} else if(frestoData.dataUnit.isSetEntryOperationReturnEdge()) {
				EntryOperationReturnEdge entryOperationReturnEdge = frestoData.dataUnit.getEntryOperationReturnEdge();
				ResourceID resourceId = entryOperationReturnEdge.resourceId;
				OperationID operationId = entryOperationReturnEdge.operationId;

				OSQLSynchQuery<ODocument> oQuery = new OSQLSynchQuery<ODocument>();
				Map<String, Object> params = new HashMap<String, Object>();

				oQuery.setText("select from ManagedResource where servletPath = :servletPath");
				params.clear();
				params.put("servletPath", entryOperationReturnEdge.servletPath);
				ODocument managedResourceV = findOne(oGraph, oQuery, params);

				if(managedResourceV == null) {
					managedResourceV = oGraph.createVertex("ManagedResource")
						.field("servletPath", entryOperationReturnEdge.servletPath);
				}

				oQuery.setText("select from Operation where operationName = :operationName and typeName = :typeName");
				params.clear();
				params.put("operationName", operationId.getOperationName());
				params.put("typeName", operationId.getTypeName());
				ODocument operationV = findOne(oGraph, oQuery, params);

				if(operationV == null) {
					operationV = oGraph.createVertex("Operation")
						.field("operationName", operationId.getOperationName())
						.field("typeName", operationId.getTypeName());
				}

				ODocument entryOperationReturnE = oGraph.createEdge(operationV, managedResourceV, "EntryOperationReturnEdge")
					.field("httpStatus", entryOperationReturnEdge.httpStatus)
					.field("timestamp", entryOperationReturnEdge.timestamp)
					.field("elapsedTime", entryOperationReturnEdge.elapsedTime)
					.field("uuid", entryOperationReturnEdge.uuid);

				entryOperationReturnE.save();

				linkToTS(oGraph, entryOperationReturnE.getIdentity(), "entryReturn", entryOperationReturnEdge.timestamp);

				oGraph.commit();

			} else if(frestoData.dataUnit.isSetOperationCallEdge()) {
			} else if(frestoData.dataUnit.isSetOperationReturnEdge()) {
			} else {
				LOGGER.info("No data unit exist.");
			}
			
		} else {
			LOGGER.warning("Event topic: " + topic + " not recognized.");
		}
	}

        public static ODocument findOne(OGraphDatabase oGraph, OSQLSynchQuery oQuery, Map<String, Object> params) {
		List<ODocument> result = oGraph.command(oQuery).execute(params);
                for(ODocument doc: result) {
                                LOGGER.fine("Found.");
                                return doc;
                }
                return null;

        }

	public static void linkToTS(OGraphDatabase oGraph, OIdentifiable oRID, String property, long timestamp) {
		long second = (timestamp/1000) * 1000;

		OSQLSynchQuery<ODocument> oQuery = new OSQLSynchQuery<ODocument>();
		Map<String, Object> params = new HashMap<String, Object>();

		oQuery.setText("select from TS where second = :second");
		params.put("second", second);
		List<ODocument> seconds = oGraph.command(oQuery).execute(params);

		if(seconds.size() > 0) {
			Set<OIdentifiable> edgeSet = oGraph.getOutEdges(seconds.get(0).getIdentity());
			OIdentifiable edgeId = null;
			for(OIdentifiable oid : edgeSet) {
				edgeId = oid;
				break; // because only one value
			}

			ODocument inVertex = oGraph.getInVertex(edgeId);

			OCommandSQL cmd = new OCommandSQL();
			cmd.setText("update " + inVertex.getIdentity() + " add " + property + " = " + oRID);
			int updated = oGraph.command(cmd).execute();

		} else {
			LOGGER.info("Creating TS vertex...");
			OCommandSQL cmd = new OCommandSQL();
			cmd.setText("insert into Second (request, response, entryCall, entryReturn, call, return) values ([],[],[],[],[],[])");
			ODocument newSecondDoc = oGraph.command(cmd).execute();

			ODocument newTSDoc = oGraph.createVertex("TS")
				.field("second", second);

			ODocument newEdge = oGraph.createEdge(newTSDoc, newSecondDoc);

			newEdge.save();

			// call this method once again
			linkToTS(oGraph, oRID, property, timestamp);
		}
	}



        //public static Vertex findOrAddCustomVertex(OrientGraph oGraph, String klass, String key, int value) {
        //        for(Vertex v: oGraph.getVerticesOfClass(klass)) {
        //                if((Integer)v.getProperty(key) == value) {
        //                        LOGGER.fine("Found a vertex");
        //                        return v;
        //                }
        //        }

        //        Vertex v =  oGraph.addVertex("class:" + klass);
        //        v.setProperty(key, value);
        //        return v;

        //}

        //public static Vertex findOrAddCustomVertex(OrientGraph oGraph, String klass, String keyOne, String valueOne, String keyTwo, String valueTwo) {
        //        for(Vertex v: oGraph.getVerticesOfClass(klass)) {
        //                if(v.getProperty(keyOne).equals(valueOne) && v.getProperty(keyTwo).equals(valueTwo)) {
        //                        LOGGER.fine("Found a vertex");
        //                        return v;
        //                }
        //        }

        //        Vertex v =  oGraph.addVertex("class:" + klass);
        //        v.setProperty(keyOne, valueOne);
        //        v.setProperty(keyTwo, valueTwo);
        //        return v;

        //}

	//public static Edge findOrMakeEdge(OrientGraph oGraph, Vertex sourceV, Vertex targetV, String edgeType) {
	//	for(Edge e: sourceV.getEdges(Direction.OUT)) {
	//		Vertex v = e.getVertex(Direction.OUT);
	//		Set<String> keys = v.getPropertyKeys();
	//		Iterator<String> it = keys.iterator();

	//		boolean isSameVertex = true;
	//		while(it.hasNext()) {
	//			String key = it.next();
	//			if(!v.getProperty(key).equals(targetV.getProperty(key))) {
	//				isSameVertex = false;
	//				break;
	//			}
	//		}

	//		if(isSameVertex) {
	//			return e;
	//		} else {
	//			return sourceV.addEdge(edgeType, targetV);
	//		}
	//	}
	//}
}

