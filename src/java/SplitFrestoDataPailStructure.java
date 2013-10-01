package fresto.pail;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TUnion;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;

import fresto.data.FrestoData;
import fresto.data.DataUnit;

public class SplitFrestoDataPailStructure extends FrestoDataPailStructure {
	public static HashMap<Short, FieldStructure> validFieldMap = new HashMap<Short, FieldStructure>();

	static {
		for(DataUnit._Fields k: DataUnit.metaDataMap.keySet()) {
			FieldValueMetaData md = DataUnit.metaDataMap.get(k).valueMetaData;
			FieldStructure fieldStruct;
			if(md instanceof StructMetaData && ((StructMetaData) md).structClass.getName().endsWith("Property")) {
				fieldStruct = new PropertyStructure(((StructMetaData) md).structClass);
			} else {
				fieldStruct = new EdgeStructure();  // endsWith("Edge")
			}
			validFieldMap.put(k.getThriftFieldId(), fieldStruct);
		}

	}

	public List<String> getTarget(FrestoData object) {
		List<String> ret = new ArrayList<String>();
		DataUnit du = object.getDataUnit();
		short id = du.getSetField().getThriftFieldId();
		ret.add("" + id);
		validFieldMap.get(id).fillTarget(ret, du.getFieldValue());
		return ret;
	}

	public boolean isValidTarget(String[] dirs) {
		if(dirs.length == 0) return false;
		try {
			short id = Short.parseShort(dirs[0]);
			FieldStructure fieldStruct = validFieldMap.get(id);
			if(fieldStruct== null)
				return false;
			else 
				return fieldStruct.isValidTarget(dirs);
		} catch(NumberFormatException e) {
			return false;
		}
	}


	protected static interface FieldStructure {
		public boolean isValidTarget(String[] dirs);
		public void fillTarget(List<String> ret, Object val);
	}

	protected static class EdgeStructure implements FieldStructure {
		public boolean isValidTarget(String[] dirs) {
			return true;
		}
	
		public void fillTarget(List<String> ret, Object val) {
		}
	}

	protected static class PropertyStructure implements FieldStructure {
		private short valueId;
		private HashSet<Short> validIds;
	
		public PropertyStructure(Class prop) {
			try {
				Map<TFieldIdEnum, FieldMetaData> propMeta = getMetadataMap(prop);
				Class valClass = Class.forName(prop.getName() + "Value");
				valueId = getIdForClass(propMeta, valClass);
	
				validIds = new HashSet<Short>();
				Map<TFieldIdEnum, FieldMetaData> valMeta = getMetadataMap(valClass);
				for(TFieldIdEnum valId: valMeta.keySet()) {
					validIds.add(valId.getThriftFieldId());
				}
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
	
		public boolean isValidTarget(String[] dirs) {
			if(dirs.length < 2) return false;
			try {
				short s = Short.parseShort(dirs[1]);
				return validIds.contains(s);
			} catch(NumberFormatException e) {
				return false;
			}
		}
	
		public void fillTarget(List<String> ret, Object val) {

			//ret.add("" +((TUnion) ((TBase)val).getFieldValue(valueId)).getSetField().getThriftFieldId());
			ret.add("" +((TUnion) ((TBase)val).fieldForId(valueId)).getSetField().getThriftFieldId());
		}
	
		private static Map<TFieldIdEnum, FieldMetaData> getMetadataMap(Class c) {
			try {
				Object o = c.newInstance();
				return (Map) c.getField("metaDataMap").get(o);
			} catch(Exception e) {
				throw new RuntimeException(e);
			}
		}
	
		private static short getIdForClass(Map<TFieldIdEnum, FieldMetaData> meta, Class toFind) {
			for(TFieldIdEnum k: meta.keySet()) {
				FieldValueMetaData md = meta.get(k).valueMetaData;
				if(md instanceof StructMetaData) {
					if(toFind.equals(((StructMetaData) md).structClass)) {
						return k.getThriftFieldId();
					}
				}
			}
			throw new RuntimeException("Could not find " + toFind.toString() + " in " + meta.toString());
		}
	}
}
