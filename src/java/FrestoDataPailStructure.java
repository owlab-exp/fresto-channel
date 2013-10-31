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
//package fresto.pail;

import fresto.data.FrestoData;

import java.util.Collections;
import java.util.List;

public class FrestoDataPailStructure extends ThriftPailStructure<FrestoData> {
	public Class getType() {
		return FrestoData.class;
	}

	protected FrestoData createThriftObject() {
		return new FrestoData();
	}

	public List<String> getTarget(FrestoData object) {
		return Collections.EMPTY_LIST;
	}

	public boolean isValidTarget(String... dirs) {
		return true;
	}
}
