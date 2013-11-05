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
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import java.util.logging.Logger;

public  class EventRelay extends  Thread implements Runnable {
	private static String THIS_CLASS_NAME = "EventRelay";
	private static Logger LOGGER = Logger.getLogger(THIS_CLASS_NAME);

	private static String FRONT_HOST = "*"; // This host
	private static int FRONT_PORT = 7000; // 7000
	private static int BACK_PORT = 7001; // 7001
	private ZMQ.Context ctx;

	public  static void main(String[] args) {
		// Ne
		if(args.length < 3) {
			LOGGER.info("Possible arguments: <front host> <front port> <back port>");
			System.exit(1);
		} else {
			try {
				FRONT_HOST = args[0];
				FRONT_PORT = Integer.parseInt(args[1]);
				BACK_PORT = Integer.parseInt(args[2]);
			} catch(NumberFormatException nfe) {
				System.err.println("Arguments must be integers.");
				System.exit(1);
			}
		}
			

		final ZMQ.Context context = ZMQ.context(1);

		final Thread zmqThread = new Thread() {
			@Override
			public void run() {

				//ZMQ.Socket frontEnd = context.socket(ZMQ.XSUB);
				ZMQ.Socket frontEnd = context.socket(ZMQ.SUB);
				frontEnd.connect("tcp://" + FRONT_HOST + ":" + FRONT_PORT);

				//ZMQ.Socket backEnd = context.socket(ZMQ.XPUB);
				ZMQ.Socket backEnd = context.socket(ZMQ.PUB);
				backEnd.bind("tcp://*:" + BACK_PORT);

				LOGGER.info("Starting Forwarder with " + FRONT_HOST + ":" + FRONT_PORT + "/" + BACK_PORT);

				frontEnd.subscribe("".getBytes());
				
				// Working!
				//ZMQ.proxy(frontEnd, backEnd, null);
				
				// Working!
				ZMQ.device(ZMQ.FORWARDER, frontEnd, backEnd);

				frontEnd.close();
				backEnd.close();
			}
		};

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOGGER.info("Interrupt received. Shuting down server");
				context.term();
				
				try {
					zmqThread.interrupt();
					zmqThread.join();
				} catch(InterruptedException e) {
					LOGGER.warning("Exception occurred: " + e.getMessage());
				}
			}
		});

		zmqThread.start();
		//context.term();
	}
}
