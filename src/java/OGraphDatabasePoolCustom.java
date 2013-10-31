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

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.db.ODatabasePoolBase;
//import com.orientechnologies.orient.core.db.graph.OGraphDatabasePooled;

public class OGraphDatabasePoolCustom extends ODatabasePoolBase<OGraphDatabase> {
	private static OGraphDatabasePoolCustom globalInstance = new OGraphDatabasePoolCustom();

	private OGraphDatabasePoolCustom() {
		super();
	}

	public static OGraphDatabasePoolCustom global(int min, int max) {
		globalInstance.setup(min, max);
		return globalInstance;
	}

	@Override
	protected OGraphDatabase createResource(Object owner, String iDatabaseName, Object... iAdditionalArgs) {
		return new OGraphDatabasePooledCustom((OGraphDatabasePoolCustom) owner, iDatabaseName, (String) iAdditionalArgs[0], (String) iAdditionalArgs[1]);
	}
}
