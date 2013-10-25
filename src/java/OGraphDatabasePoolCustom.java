
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
