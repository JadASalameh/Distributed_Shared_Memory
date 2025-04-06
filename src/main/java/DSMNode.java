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
    private final PartitionConfig partitionConfig;
    private final MessagingService messagingService;

    @JsonCreator
    public DSMNode(
            @JsonProperty("name") String name,
            @JsonProperty("startAddress") int startAddress,
            @JsonProperty("endAddress") int endAddress,
            @JsonProperty("primary") boolean isPrimary,
            @JsonProperty("replicaNodes") List<String> replicaNodes,
            PartitionConfig config,
            MessagingService messaging) {
        this.name = name;
        this.startAddress = startAddress;
        this.endAddress = endAddress;
        this.isPrimary = isPrimary;
        this.replicaNodes = replicaNodes;
        this.storage = new HashMap<>();
        this.partitionConfig = config;
        this.messagingService = messaging;
        initializeStorage();
    }

    private void initializeStorage() {
        for (int addr = startAddress; addr <= endAddress; addr++) {
            storage.put(addr, 0);
        }
    }

    public boolean isLocalAddress(Address address) {
        int addrValue = address.getValue();
        return addrValue >= startAddress && addrValue <= endAddress;
    }

    private void handleMessage(DSMMessage msg) {
        try {
            if (!isLocalAddress(msg.getAddress())) {
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
            // Pick a random node from the group for read
            targetNode = group.get(new Random().nextInt(group.size()));
        } else {
            // For write or replicate, always forward to primary
            targetNode = group.getFirst();
        }

        try {
            messagingService.send(targetNode, msg);
        } catch (IOException e) {
            System.err.println("Failed to forward message to " + targetNode + ": " + e.getMessage());
        }
    }


    // Getters
    public String getName() { return name; }
    public int getStartAddress() { return startAddress; }
    public int getEndAddress() { return endAddress; }
    public boolean isPrimary() { return isPrimary; }
    public List<String> getReplicaNodes() { return replicaNodes; }
}

