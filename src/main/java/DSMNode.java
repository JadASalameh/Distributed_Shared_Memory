import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.*;

public class DSMNode {
    private final String name;
    private final int startAddress;
    private final int endAddress;
    private final Map<Integer, Integer> storage;
    private final boolean isPrimary;
    private final List<String> replicaNodes;


    private PartitionConfig partitionConfig;
    private MessagingService messagingService;


    @JsonCreator
    public DSMNode(
            @JsonProperty("name") String name,
            @JsonProperty("startAddress") int startAddress,
            @JsonProperty("endAddress") int endAddress,
            @JsonProperty("primary") boolean isPrimary,
            @JsonProperty("replicaNodes") List<String> replicaNodes) {
        this.name = name;
        this.startAddress = startAddress;
        this.endAddress = endAddress;
        this.isPrimary = isPrimary;
        this.replicaNodes = replicaNodes;
        this.storage = new HashMap<>();
        // Set these to null initially; they will be injected later.
        this.partitionConfig = null;
        this.messagingService = null;
        initializeStorage();
    }

    private void initializeStorage() {
        for (int addr = startAddress; addr <= endAddress; addr++) {
            storage.put(addr, 0);
        }
    }

    // Setter to inject PartitionConfig after deserialization.
    public void setPartitionConfig(PartitionConfig config) {
        this.partitionConfig = config;
    }

    // Setter to inject MessagingService after deserialization.
    public void setMessagingService(MessagingService messaging) {
        this.messagingService = messaging;
    }

    public boolean isLocalAddress(Address address) {
        int addrValue = address.getValue();
        return addrValue >= startAddress && addrValue <= endAddress;
    }

    private void handleMessage(DSMMessage msg) {
        try {
            if (!isLocalAddress(msg.getAddress())) {
                System.out.println("[" + name + "] Forwarding " + msg.getType() + " for address " + msg.getAddress().getValue());
                forwardMessage(msg);
                return;
            }

            switch (msg.getType()) {
                case WRITE -> handleWrite(msg);
                case READ -> handleRead(msg);
                case REPLICATE -> handleReplicate(msg);
            }
        } catch (Exception e) {
            System.err.println("Error handling message: " + e.getMessage());
        }
    }

    private void handleWrite(DSMMessage msg) {
        int value = Integer.parseInt(msg.getValue());
        storage.put(msg.getAddress().getValue(), value);
        System.out.println("[" + name + "] WROTE value " + msg.getValue() + " at address " + msg.getAddress().getValue());

        if (isPrimary) {
            replicaNodes.forEach(replica -> {
                DSMMessage replicateMsg = new DSMMessage(
                        DSMMessage.Type.REPLICATE, msg.getAddress(), msg.getValue(), null
                );
                try {
                    messagingService.send(replica, replicateMsg);

                } catch (IOException e) {
                    System.err.println("Failed to replicate to " + replica + ": " + e.getMessage());
                }
            });
        }
    }

    private void handleRead(DSMMessage msg) {
        Integer value = storage.get(msg.getAddress().getValue());
        if (value == null) value = 0;

        if (msg.getReplyToQueue() != null) {
            try {
                messagingService.sendReply(msg.getReplyToQueue(), String.valueOf(value));
                System.out.println("[" + name + "] READ value " + value + " from address " + msg.getAddress().getValue());
            } catch (IOException e) {
                System.err.println("Failed to send reply: " + e.getMessage());
            }
        }
    }

    private void handleReplicate(DSMMessage msg) {
        int value = Integer.parseInt(msg.getValue());
        storage.put(msg.getAddress().getValue(), value);
    }

    private void forwardMessage(DSMMessage msg) {
        List<String> group = partitionConfig.getReplicationGroup(msg.getAddress());
        String targetNode;
        if (msg.getType() == DSMMessage.Type.READ) {
            targetNode = group.get(new Random().nextInt(group.size()));
        } else {
            targetNode = group.get(0);
        }
        try {
            messagingService.send(targetNode, msg);
        } catch (IOException e) {
            System.err.println("Failed to forward message to " + targetNode + ": " + e.getMessage());
        }
    }

    // Optional: A helper method to start listening for DSM messages.
    // (Depends on how your MessagingService is implemented.)
    public void start() throws IOException {
        messagingService.startMessageListener(this.name, this::handleMessage);
        System.out.println("DSMNode " + name + " is now listening for messages.");
    }

    // Getters
    public String getName() { return name; }
    public int getStartAddress() { return startAddress; }
    public int getEndAddress() { return endAddress; }
    public boolean isPrimary() { return isPrimary; }
    public List<String> getReplicaNodes() { return replicaNodes; }
}

