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

public  class EventStreamer extends  Thread implements Runnable {
	private static Logger LOGGER = LoggerFactory.getLogger(EventStreamer.class);

	private static String FRONT_URL;
	private static String BACK_URL;

	public  static void main(String[] args) { 
		if(args.length < 2) { 
			LOGGER.info("Possible arguments: <front url> <back url>"); 
			System.exit(1); 
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
				//frontEnd.connect("tcp://" + FRONT_HOST + ":" + FRONT_PORT);
				frontEnd.connect(FRONT_URL);
				frontEnd.subscribe("".getBytes());

				//ZMQ.Socket backEnd = context.socket(ZMQ.XPUB);
				ZMQ.Socket backEnd = context.socket(ZMQ.PUSH);
				backEnd.bind(BACK_URL);

				LOGGER.info("Start streamer with " + FRONT_URL + " and " + BACK_URL);

				
				// Working!
				//ZMQ.proxy(frontEnd, backEnd, null);
				
				// Working!
				ZMQ.device(ZMQ.STREAMER, frontEnd, backEnd);

				frontEnd.close();
				backEnd.close();
			}
		};

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				LOGGER.info(" Interrupt received. Shutting down server...");
				context.term();

				try {
					zmqThread.interrupt();
					zmqThread.join();
				} catch(InterruptedException e) {
				}
			}
		});

		zmqThread.start();
	}
}
