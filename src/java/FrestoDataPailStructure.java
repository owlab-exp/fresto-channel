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
