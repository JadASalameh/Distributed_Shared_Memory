import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DSMMessage {
    public enum Type { WRITE, READ, REPLICATE, REPLICATE_ACK}

    private final Type type;
    private final Address address;
    private final String value;
    private final String replyToQueue;
    private final long sequenceNumber;

    @JsonCreator
    public DSMMessage(
            @JsonProperty("type") Type type,
            @JsonProperty("address") Address address,
            @JsonProperty("value") String value,
            @JsonProperty("replyToQueue") String replyToQueue,
            @JsonProperty("sequenceNumber") long sequenceNumber) {
        this.type = type;
        this.address = address;
        this.value = value;
        this.replyToQueue = replyToQueue;
        this.sequenceNumber = sequenceNumber;
    }

    // Getters
    public Type getType() { return type; }
    public Address getAddress() { return address; }
    public String getValue() { return value; }
    public String getReplyToQueue() { return replyToQueue; }
    public long getSequenceNumber() {return sequenceNumber;}
}