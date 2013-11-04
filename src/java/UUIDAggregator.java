/**************************************************************************************
 * Copyright 2013 TheSystemIdeas, Inc and Contributors. All rights reserved.          *
 *                                                                                    *
 *     https://github.com/owlab/fresto                                                *
 *                                                                                    *
 *                                                                                    *
 * ---------------------------------------------------------------------------------- *
 * This file is licensed under the Apache License, Version 2.0 (the "License");       *
 * you may not use this file except in compliance with the License.                   *
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 * 
 **************************************************************************************/
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
import fresto.data.SqlID;
import fresto.data.RequestEdge;
import fresto.data.ResponseEdge;
import fresto.data.EntryOperationCallEdge;
import fresto.data.EntryOperationReturnEdge;
import fresto.data.OperationCallEdge;
import fresto.data.OperationReturnEdge;
import fresto.data.SqlCallEdge;
import fresto.data.SqlReturnEdge;
import fresto.command.CommandEvent;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;

//import org.perf4j.LoggingStopWatch;
//import org.perf4j.javalog.JavaLogStopWatch;
//import org.perf4j.StopWatch;

/**
 * Aggregate individual events by UUID and sequence
 */
public class UUIDAggregator {
	private static String THIS_CLASS_NAME = "UUIDAggregator";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);

	private static String frontHost;
	private static int frontPort;
	private static String dbHost;
	private static String dbName;
	private static String dbUser;
	private static String password;

	// Client Events
	private static final String TOPIC_REQUEST = "CB";
	private static final String TOPIC_RESPONSE = "CF";

	// Server Events
	private static final String TOPIC_ENTRY_CALL = "EB";
	private static final String TOPIC_ENTRY_RETURN = "EF";
	private static final String TOPIC_OPERATION_CALL = "OB";
	private static final String TOPIC_OPERATION_RETURN = "OF";
	private static final String TOPIC_SQL_CALL = "SB";
	private static final String TOPIC_SQL_RETURN = "SF";

	
	private static TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
	

	private static boolean work = true;
	private static boolean sleepOn = false;
	private static int SLEEP_TIME = 10;

	private static OGraphDatabase oGraph;

	public UUIDAggregator() {

	}

	public static void main(String[] args) throws Exception {
		if(args.length <  6) {
			LOGGER.severe("Argumests needed : <frontHost> <frontPort> <nbHost> <nbName> <dbUser> <password>");
			System.exit(1);
		} else {
			try { 
				frontHost = args[0];
				frontPort = Integer.parseInt(args[1]); 
				dbHost = args[2];
				dbName = args[3];
				dbUser = args[4];
				password = args[5];
			} catch(NumberFormatException e) {
				LOGGER.severe("frontPort shouldb be an integer.");
				System.exit(1);
			}
		}

		final ZMQ.Context context = ZMQ.context(1);

		final FrestoEventQueue frestoEventQueue = new FrestoEventQueue();

		final Thread aggregatorThread = new Thread() {
				Logger _LOGGER = Logger.getLogger("aggregatorThread");
				//StopWatch _watch = new JavaLogStopWatch(_LOGGER);
				FrestoStopWatch _watch = new FrestoStopWatch();
			@Override
			public void run() {
				UUIDAggregator aggregator = new UUIDAggregator();


				// Open database
				aggregator.setupDBConnection();
				if(oGraph.isClosed()) {
					oGraph.open(dbUser, password);
					_LOGGER.info("[Open DB] " + _watch.lap() + " ms");
				}


				ZMQ.Socket puller = context.socket(ZMQ.PULL);
				puller.connect("tcp://" + frontHost + ":" + frontPort);

				//Consume socket data
				frestoEventQueue.setPullerSocket(puller);
				frestoEventQueue.start();

				int writeCount = 0;

				_watch.start();
				while(work) {

					// To add sufficient events to the queue
					//if(sleepOn) {
					if(frestoEventQueue.isEmpty()) {
						try {
							_LOGGER.info("frestoEventQueue is empty. Waiting " + SLEEP_TIME + "ms...");
							Thread.sleep(SLEEP_TIME);
							continue;
						} catch(InterruptedException ie) {
						}
					}

					//int queueSize = frestoEventQueue.size();
					
					//if(queueSize > 0) {

						//_watch.start();
						if(oGraph.isClosed()) {
							oGraph.open(dbUser, password);
							_LOGGER.info("[Open DB] " + _watch.lap() + " ms.");// queueSize=" + queueSize);
						}

						try { // for database close finally

							//for(int i = 0; i < queueSize; i++) {
								FrestoEvent frestoEvent = frestoEventQueue.poll(); 
								try {
									aggregator.allocateEventData(frestoEvent.topic, frestoEvent.eventBytes);
									writeCount++;
								} catch(Exception te) {
									te.printStackTrace();
								}
							//}
						} finally {
							//oGraph.close();
						}

						if(writeCount == 1000) {
							_LOGGER.info("time[" + _watch.lap() + "] " + writeCount + " event processed. Queue size = " + frestoEventQueue.size());
							writeCount = 0;
						}
						//_LOGGER.info("time[" + _watch.stop() + "] " + queueSize + " events processed");

						//_LOGGER.info(queueSize + " events processed.");
					//} else {
					//	_LOGGER.fine("No events.");

					//}

				}
				_LOGGER.info("Shutting down...");

				oGraph.close();

				puller.close();
				context.term();

				_LOGGER.info("Good bye.");
			}
		};

		Runtime.getRuntime().addShutdownHook(new Thread() {
         		@Override
         		public void run() {
         		   System.out.println("Interrupt received, killing server¡¦");
			   // To break while clause
			   frestoEventQueue.stopWork();
			   work = false;

         		  try {
				  frestoEventQueue.join();
				  aggregatorThread.join();

         		  } catch (InterruptedException e) {
         		  }
         		}
      		});

		aggregatorThread.start();

	}

	public OGraphDatabase setupDBConnection() {
		//OGraphDatabase oGraph = new OGraphDatabase(DB_URL);
		LOGGER.info("Setting up connection to DB"); 
		oGraph = new OGraphDatabase("remote:"+ dbHost + "/" + dbName);
		oGraph.setProperty("minPool", 1);
		oGraph.setProperty("maxPool", 3);

		LOGGER.info("oGraph=" + oGraph); 
		return oGraph;
	}

	public void allocateEventData(String topic, byte[] eventBytes) throws TException, IOException {
		if(TOPIC_REQUEST.equals(topic) 
			|| TOPIC_RESPONSE.equals(topic)
			|| TOPIC_ENTRY_CALL.equals(topic)
			|| TOPIC_ENTRY_RETURN.equals(topic)
			|| TOPIC_OPERATION_CALL.equals(topic)
			|| TOPIC_OPERATION_RETURN.equals(topic)
			|| TOPIC_SQL_CALL.equals(topic)
			|| TOPIC_SQL_RETURN.equals(topic)
			) {

			//StopWatch _watch = new LoggingStopWatch("allocateEventData");

			FrestoData frestoData = new FrestoData();
			//Reuse
			//frestoData.clear();
			deserializer.deserialize(frestoData, eventBytes);
			//_watch.lap("deserialize eventBytes");

			Pedigree pedigree = new Pedigree();
                        pedigree.setReceivedTime(System.currentTimeMillis());

                        frestoData.setPedigree(pedigree);
			//_watch.lap("setting pedigree");

			if(frestoData.dataUnit.isSetRequestEdge()) {

				RequestEdge requestEdge = frestoData.dataUnit.getRequestEdge();
				ClientID clientId = requestEdge.clientId;
				ResourceID resourceId = requestEdge.resourceId;

				//requestMap.put(requestEdge.uuid, frestoData.dataUnit);
				
				//
				//StopWatch _watch = new LoggingStopWatch("Writing Request Event");

				ODocument request = oGraph.createVertex("Request")
					.field("clientIp", clientId.getClientIp())
					.field("url", resourceId.getUrl())
					.field("referrer", requestEdge.referrer)
					.field("method", requestEdge.method)
					.field("timestamp", requestEdge.timestamp)
					.field("uuid", requestEdge.uuid)
					.save();

				//_watch.lap("Request event processed");
				linkToUUID(oGraph, requestEdge.uuid, "Request", request.getIdentity());
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetResponseEdge()) {

				ResponseEdge responseEdge = frestoData.dataUnit.getResponseEdge();
				ClientID clientId = responseEdge.clientId;
				ResourceID resourceId = responseEdge.resourceId;

				//responseMap.put(responseEdge.uuid, frestoData.dataUnit);
				//uuidMap.add(responseEdge.uuid);

				//StopWatch _watch = new LoggingStopWatch("Writing Response Event");

				ODocument response = oGraph.createVertex("Response")
					.field("clientIp", clientId.getClientIp())
					.field("url", resourceId.getUrl())
					.field("httpStatus", responseEdge.httpStatus)
					.field("elapsedTime", responseEdge.elapsedTime)
					.field("timestamp", responseEdge.timestamp)
					.field("uuid", responseEdge.uuid)
					.save();

				//_watch.lap("Response event processed");
				linkToUUID(oGraph, responseEdge.uuid, "Respone", response.getIdentity());
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetEntryOperationCallEdge()) {

				EntryOperationCallEdge entryOperationCallEdge = frestoData.dataUnit.getEntryOperationCallEdge();
				ResourceID resourceId = entryOperationCallEdge.resourceId;
				OperationID operationId = entryOperationCallEdge.operationId;

				//entryOperationCallMap.put(entryOperationCallEdge.uuid, frestoData.dataUnit);

				//StopWatch _watch = new LoggingStopWatch("Writing EntryOperationCall");

				ODocument entryCall = oGraph.createVertex("EntryOperationCall")
					.field("hostName", entryOperationCallEdge.localHost)
					.field("contextPath", entryOperationCallEdge.contextPath)
					.field("port", entryOperationCallEdge.localPort)
					.field("servletPath", entryOperationCallEdge.servletPath)
					.field("operationName", operationId.getOperationName())
					.field("typeName", operationId.getTypeName())
					.field("httpMethod", entryOperationCallEdge.httpMethod)
					.field("uuid", entryOperationCallEdge.uuid)
					.field("timestamp", entryOperationCallEdge.timestamp)
					.field("sequence", entryOperationCallEdge.sequence)
					.field("depth", entryOperationCallEdge.depth)
					.save();


				//_watch.lap("EntryOperationCall event processed");
				linkToUUID(oGraph, entryOperationCallEdge.uuid, "EntryOperationCall", entryCall.getIdentity());
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetEntryOperationReturnEdge()) {

				EntryOperationReturnEdge entryOperationReturnEdge = frestoData.dataUnit.getEntryOperationReturnEdge();
				ResourceID resourceId = entryOperationReturnEdge.resourceId;
				OperationID operationId = entryOperationReturnEdge.operationId;
				//
				//entryOperationReturnMap.put(entryOperationReturnEdge.uuid, frestoData.dataUnit);

				//StopWatch _watch = new LoggingStopWatch("Writing EntryOperationReturn");

				ODocument entryReturn = oGraph.createVertex("EntryOperationReturn")
					.field("servletlPath", entryOperationReturnEdge.servletPath)
					.field("operationName", operationId.getOperationName())
					.field("typeName", operationId.getTypeName())
					.field("httpStatus", entryOperationReturnEdge.httpStatus)
					.field("timestamp", entryOperationReturnEdge.timestamp)
					.field("elapsedTime", entryOperationReturnEdge.elapsedTime)
					.field("uuid", entryOperationReturnEdge.uuid)
					.field("sequence", entryOperationReturnEdge.sequence)
					.field("depth", entryOperationReturnEdge.depth)
					.save();

				//_watch.lap("EntryOperationReturn event processed");
				linkToUUID(oGraph, entryOperationReturnEdge.uuid, "EntryOperationReturn", entryReturn.getIdentity());
				//_watch.stop("Link event processed");


			} else if(frestoData.dataUnit.isSetOperationCallEdge()) {

				OperationCallEdge operationCallEdge = frestoData.dataUnit.getOperationCallEdge();
				OperationID operationId = operationCallEdge.operationId;


				//Map<Integer, DataUnit> operationCallSeqMap = operationCallMap.putIfAbsent(operationCallEdge.uuid, new ConcurrentSkipListMap<Integer, DataUnit>());
				//if(operationCallSeqMap != null) {
				//	operationCallSeqMap.put(operationCallEdge.sequence, frestoData.dataUnit);
				//} else {
				//	operationCallSeqMap = operationCallMap.get(operationCallEdge.uuid);
				//	operationCallSeqMap.put(operationCallEdge.sequence, frestoData.dataUnit);
				//}
				//
				//StopWatch _watch = new LoggingStopWatch("Writing OperationCall");

				ODocument operationCall = oGraph.createVertex("OperationCall")
					.field("operationName", operationId.getOperationName())
					.field("typeName", operationId.getTypeName())
					.field("timestamp", operationCallEdge.timestamp)
					.field("uuid", operationCallEdge.uuid)
					.field("depth", operationCallEdge.depth)
					.field("sequence", operationCallEdge.sequence)
					.save();

				//_watch.lap("OperationCall event processed");
				linkToUUID(oGraph, operationCallEdge.uuid, "OperationCall", operationCall.getIdentity());
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetOperationReturnEdge()) {
				OperationReturnEdge operationReturnEdge = frestoData.dataUnit.getOperationReturnEdge();
				OperationID operationId = operationReturnEdge.operationId;

				//StopWatch _watch = new LoggingStopWatch("Writing OperationReturn");

				//Map<Integer, DataUnit> operationReturnSeqMap = operationReturnMap.putIfAbsent(operationReturnEdge.uuid, new ConcurrentSkipListMap<Integer, DataUnit>());
				//if(operationReturnSeqMap != null) {
				//	operationReturnSeqMap.put(operationReturnEdge.sequence, frestoData.dataUnit);
				//} else {
				//	operationReturnMap.get(operationReturnEdge.uuid).put(operationReturnEdge.sequence, frestoData.dataUnit);
				//}
				//
				ODocument operationReturn = oGraph.createVertex("OperationReturn")
					.field("operationName", operationId.getOperationName())
					.field("typeName", operationId.getTypeName())
					.field("timestamp", operationReturnEdge.timestamp)
					.field("elapsedTime", operationReturnEdge.elapsedTime)
					.field("uuid", operationReturnEdge.uuid)
					.field("sequence", operationReturnEdge.sequence)
					.field("depth", operationReturnEdge.depth)
					.save();

				//_watch.lap("OperationReturn event processed");
				linkToUUID(oGraph, operationReturnEdge.uuid, "OperationReturn", operationReturn.getIdentity());
				//_watch.stop("Link event processed");
			} else if(frestoData.dataUnit.isSetSqlCallEdge()) {

				SqlCallEdge sqlCallEdge = frestoData.dataUnit.getSqlCallEdge();
				SqlID sqlId = sqlCallEdge.sqlId;

				//Map<Integer, DataUnit> sqlCallSeqMap = sqlCallMap.putIfAbsent(sqlCallEdge.uuid, new ConcurrentSkipListMap<Integer, DataUnit>());
				//if(sqlCallSeqMap != null) {
				//	sqlCallSeqMap.put(sqlCallEdge.sequence, frestoData.dataUnit);
				//} else {
				//	sqlCallMap.get(sqlCallEdge.uuid).put(sqlCallEdge.sequence, frestoData.dataUnit);
				//}
				////Map<Integer, DataUnit> sqlCallSeqMap = sqlCallMap.get(sqlCallEdge.uuid);
				////if(sqlCallSeqMap != null) {
				////	//sqlCallSeqMap.put(sqlCallEdge.sequence, sqlCallEdge);
				////	sqlCallSeqMap.put(sqlCallEdge.sequence, frestoData.dataUnit);
				////} else {
				////	sqlCallSeqMap = new ConcurrentHashMap<Integer, DataUnit>();
				////	sqlCallSeqMap.put(sqlCallEdge.sequence, frestoData.dataUnit);
				////	sqlCallMap.put(sqlCallEdge.uuid, sqlCallSeqMap);
				////}
				//StopWatch _watch = new LoggingStopWatch("Writing SqlCall");

				ODocument sqlCall = oGraph.createVertex("SqlCall")
					.field("databaseUrl", sqlId.getDatabaseUrl())
					.field("sql", sqlId.getSql())
					.field("timestamp", sqlCallEdge.timestamp)
					.field("uuid", sqlCallEdge.uuid)
					.field("depth", sqlCallEdge.depth)
					.field("sequence", sqlCallEdge.sequence)
					.save();

				//_watch.lap("SqlCall event processed");
				linkToUUID(oGraph, sqlCallEdge.uuid, "SqlCall", sqlCall.getIdentity());
				//_watch.stop("Link event processed");

			} else if(frestoData.dataUnit.isSetSqlReturnEdge()) {
				SqlReturnEdge sqlReturnEdge = frestoData.dataUnit.getSqlReturnEdge();
				SqlID sqlId = sqlReturnEdge.sqlId;

				//Map<Integer, DataUnit> sqlReturnSeqMap = sqlReturnMap.putIfAbsent(sqlReturnEdge.uuid, new ConcurrentSkipListMap<Integer, DataUnit>());
				//if(sqlReturnSeqMap != null) {
				//	sqlReturnSeqMap.put(sqlReturnEdge.sequence, frestoData.dataUnit);
				//} else {
				//	sqlReturnMap.get(sqlReturnEdge.uuid).put(sqlReturnEdge.sequence, frestoData.dataUnit);
				//}
				////Map<Integer, DataUnit> sqlReturnSeqMap = sqlReturnMap.get(sqlReturnEdge.uuid);
				////if(sqlReturnSeqMap != null) {
				////	//sqlReturnSeqMap.put(sqlReturnEdge.sequence, sqlReturnEdge);
				////	sqlReturnSeqMap.put(sqlReturnEdge.sequence, frestoData.dataUnit);
				////} else {
				////	sqlReturnSeqMap = new ConcurrentHashMap<Integer, DataUnit>();
				////	sqlReturnSeqMap.put(sqlReturnEdge.sequence, frestoData.dataUnit);
				////	sqlReturnMap.put(sqlReturnEdge.uuid, sqlReturnSeqMap);
				////}
				//StopWatch _watch = new LoggingStopWatch("Writing SqlReturn");

				ODocument sqlReturn = oGraph.createVertex("SqlReturn")
					.field("databaseUrl", sqlId.getDatabaseUrl())
					.field("sql", sqlId.getSql())
					.field("timestamp", sqlReturnEdge.timestamp)
					.field("elapsedTime", sqlReturnEdge.elapsedTime)
					.field("uuid", sqlReturnEdge.uuid)
					.field("depth", sqlReturnEdge.depth)
					.field("sequence", sqlReturnEdge.sequence)
					.save();

				//_watch.lap("SqlReturn event processed");
				linkToUUID(oGraph, sqlReturnEdge.uuid, "SqlReturn", sqlReturn.getIdentity());
				//_watch.stop("Link event processed");
			} else {
				LOGGER.info("No data unit exist.");
			}

			
		} else {
			LOGGER.warning("Event topic: " + topic + " not recognized.");
		}
	}

	public static ORID lookForVertex(OGraphDatabase oGraph, String indexName, Object key) {
		ORID oRID = null;
		OIndex<?> idx = oGraph.getMetadata().getIndexManager().getIndex(indexName);
		if(idx != null) {
			oRID = (ORID) idx.get(key);
			if(oRID == null) {
				ODocument uuidDoc = oGraph.createVertex("UUIDs").field("uuid", key).save();
				oRID = uuidDoc.getIdentity();
			}
		} else {
			LOGGER.info("INDEX: " + idx + " does not exist");
		}

		return oRID;
	}

	public static void linkToUUID(OGraphDatabase oGraph, String uuid, String className, ORID oRID) {

		ORID uuidORID = lookForVertex(oGraph, "UUIDs.uuid", uuid);
		oGraph.createEdge(uuidORID, oRID).field("className", className).save();
	}
}

