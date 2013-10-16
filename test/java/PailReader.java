import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.io.StringWriter;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.SequenceFileFormat;
import com.backtype.hadoop.pail.Pail.TypedRecordOutputStream;

import fresto.data.FrestoData;

public class PailReader {
	private static final String path = "hdfs://fresto1.owlab.com:9000/fresto/new";
	public static void main(String[] args) throws IOException {

		readData();	
	}

	public static void readData() throws IOException {
		Pail<FrestoData> frestoDataPail = new Pail<FrestoData>(path);
		for(FrestoData fd: frestoDataPail) {
			System.out.println(fd.pedigree.receivedTime);
		}

	}

//	public static void simpleIO() throws IOException {
//		Pail pail = Pail.create("hdfs://fresto1:9000/tutorial/mypail");
//		TypedRecordOutputStream os = pail.openWrite();
//		os.writeObject(new byte[] {1, 2, 3});
//		os.writeObject(new byte[] {1, 2, 3, 4});
//		os.writeObject(new byte[] {1, 2, 3, 4, 5});
//		os.close();
//	}
//
//	public static void writeLogins() throws IOException {
//		Pail<Login> loginPail = Pail.create("hdfs://fresto1:9000/tutorial/tmp/logins", new LoginPailStructure());
//		TypedRecordOutputStream out = loginPail.openWrite();
//		out.writeObject(new Login("alex", 1352679231));
//		out.writeObject(new Login("bob", 1352679216));
//		out.close();
//	}
//
//	public static void readLogins() throws IOException {
//		Pail<Login> loginPail = new Pail<Login>("hdfs://fresto1:9000/tutorial/tmp/logins");
//		for(Login l: loginPail) {
//			System.out.println(l.userName + " " + l.loginUnixTime);
//		}
//	}
//
//	public static void anotherLogins() throws IOException {
//		Pail<Login> loginPail = Pail.create("hdfs://fresto1:9000/tutorial/tmp/updates", new LoginPailStructure());
//		TypedRecordOutputStream out = loginPail.openWrite();
//		out.writeObject(new Login("teri", 1352679231));
//		out.writeObject(new Login("john", 1352679216));
//		out.close();
//	}
//
//	public static void appendData() throws IOException {
//		Pail<Login> loginPail = new Pail<Login>("hdfs://fresto1:9000/tutorial/tmp/logins");
//		Pail<Login> updatePail = new Pail<Login>("hdfs://fresto1:9000/tutorial/tmp/updates");
//		loginPail.absorb(updatePail);
//	}
//
//	public static void partitionData() throws IOException {
//		Pail<Login> pail = Pail.create("hdfs://fresto1:9000/tutorial/tmp/partitioned_logins", new PartitionedLoginPailStructure());
//		TypedRecordOutputStream os = pail.openWrite();
//		os.writeObject(new Login("chris", 1352702020));
//		os.writeObject(new Login("david", 1352788472));
//		os.close();
//	}
//
//	public static void createCompressedPail() throws IOException {
//		Map<String, Object> options = new HashMap<String, Object>();
//		options.put(SequenceFileFormat.CODEC_ARG, SequenceFileFormat.CODEC_ARG_GZIP);
//		options.put(SequenceFileFormat.TYPE_ARG, SequenceFileFormat.TYPE_ARG_BLOCK);
//
//		LoginPailStructure struct = new LoginPailStructure();
//		Pail compressed = Pail.create("/tmp/compressed", new PailSpec("SequenceFile", options, struct));
//	}

}
