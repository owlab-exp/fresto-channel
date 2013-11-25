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
package fresto.channel;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public  class EventHub extends  Thread implements Runnable {
	private static String THIS_CLASS_NAME = "EventHub";
	private static Logger LOGGER = LoggerFactory.getLogger(EventHub.class);

	private static String FRONT_URL = "tcp://*:7000";
	private static String BACK_URL = "tcp://*:7001";

	//private static String FRONT_BIND_HOST = "*"; // 7000
	//private static String FRONT_BIND_PORT = "7000"; // 7000
	//private static String BACK_BIND_HOST = "*"; // 7001
	//private static String BACK_BIND_PORT = "7001"; // 7001
	private ZMQ.Context ctx;

	public  static void main(String[] args) {
		// Ne
		if(args.length < 2) {
			LOGGER.info("Possible arguments: <front bind url> <back bind url>");
			LOGGER.info("Default front(" + FRONT_URL + ")/back(" + BACK_URL +") will be used.");
		} else {
			FRONT_URL = args[0];
			BACK_URL = args[1];
		}
			

		final ZMQ.Context context = ZMQ.context(1);

		final Thread zmqThread = new Thread() {
			@Override
			public void run() {

				//ZMQ.Socket frontEnd = context.socket(ZMQ.XSUB);
				ZMQ.Socket frontEnd = context.socket(ZMQ.SUB);
				//frontEnd.bind("tcp://" + FRONT_BIND_HOST + ":" + FRONT_BIND_PORT);
				frontEnd.bind(FRONT_URL);

				//ZMQ.Socket backEnd = context.socket(ZMQ.XPUB);
				ZMQ.Socket backEnd = context.socket(ZMQ.PUB);
				//backEnd.bind("tcp://" + BACK_BIND_HOST + ":" + BACK_BIND_PORT);
				backEnd.bind(BACK_URL);


				frontEnd.subscribe("".getBytes());
				
				LOGGER.info("Event forwarder start with " + FRONT_URL + " and " + BACK_URL);
				
				// Working!
				ZMQ.device(ZMQ.FORWARDER, frontEnd, backEnd);


				frontEnd.close();
				backEnd.close();
			}
		};

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOGGER.info(" Interrupt received. Shuting down server");
				context.term();
				
				try {
					zmqThread.interrupt();
					zmqThread.join();
				} catch(InterruptedException e) {
					LOGGER.warn("Exception occurred: " + e.getMessage());
				}
			}
		});

		zmqThread.start();
		//context.term();
	}
}
