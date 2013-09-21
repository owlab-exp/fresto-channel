import com.backtype.hadoop.pail.PailStructure;

import org.apache.thrift.TBase;
import org.apache.thrift.TSerializer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

public abstract class ThriftPailStructure<T extends Comparable> implements PailStructure<T> {
	private transient TSerializer serializer;
	private transient TDeserializer deserializer;

	private TSerializer getSerializer() {
		if(serializer == null) 
			serializer = new TSerializer();
		return serializer;
	}

	private TDeserializer getDeserializer() {
		if(deserializer == null) {
			deserializer = new TDeserializer();
		}
		return deserializer;
	}

	public byte[] serialize(T object) {
		try {
			return getSerializer().serialize((TBase) object);
		} catch(TException e) {
			throw new RuntimeException(e);
		}
	}

	public T deserialize(byte[] record) {
		T object = createThriftObject();
		try {
			getDeserializer().deserialize((TBase) object, record);
		} catch(TException e) {
			throw new RuntimeException(e);
		}
		return object;
	}

	protected abstract T createThriftObject();
}
