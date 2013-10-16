public class FrestoEvent {
	public String topic;
	public byte[] eventBytes;
	public FrestoEvent(String topic, byte[] eventBytes){
		this.topic = topic;
		this.eventBytes = eventBytes;
	}
}
