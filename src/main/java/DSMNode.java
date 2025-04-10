import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DSMNode {
    private final String name;
    private final int startAddress;
    private final int endAddress;
    private final Map<Integer, Integer> storage;
    private final boolean isPrimary;
    private final List<String> replicaNodes;


    private PartitionConfig partitionConfig;
    private MessagingService messagingService;

    private final Map<Address, Integer> pendingReplications = new ConcurrentHashMap<>();
    private final Map<Address, String> pendingClientReplies = new ConcurrentHashMap<>();

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
                case READ -> {
                    // If we are the primary for this address, forward the read to a replica
                    // If we are not the primary, actually handle it
                    if (isPrimary && !replicaNodes.isEmpty()) {
                        forwardMessage(msg);
                    } else {
                        // I'm either a replica or the only node
                        handleRead(msg);
                    }
                }
                case REPLICATE -> handleReplicate(msg);
                case REPLICATE_ACK -> handleReplicateAck(msg);
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
           Address address = msg.getAddress();
           pendingReplications.put(address, replicaNodes.size());
           if(msg.getReplyToQueue()!=null){
               pendingClientReplies.put(address,msg.getReplyToQueue());
           }
           replicaNodes.forEach(replica -> {
               DSMMessage replicateMessage = new DSMMessage(DSMMessage.Type.REPLICATE,address,msg.getValue(),this.name);
               try{
                   messagingService.send(replica,replicateMessage);
               }catch(IOException e){
                   System.out.println("Replication failed to " + replica + ": " + e.getMessage());

               }
           });
        }
    }

    private void decrementPendingReplications(Address address) {
        synchronized (pendingReplications) {
            int remaining = pendingReplications.getOrDefault(address, 0) - 1;
            if (remaining <= 0) {
                pendingReplications.remove(address);
                // Send acknowledgment to client
                String replyQueue = pendingClientReplies.remove(address);
                if (replyQueue != null) {
                    try {
                        messagingService.sendReply(replyQueue, "ACK");
                    } catch (IOException e) {
                        System.err.println("Failed to send ACK to client: " + e.getMessage());
                    }
                }
            } else {
                pendingReplications.put(address, remaining);
            }
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
        // Send ACK back to primary
        DSMMessage ackMsg = new DSMMessage(
                DSMMessage.Type.REPLICATE_ACK, msg.getAddress(), "ACK", msg.getReplyToQueue()
        );
        try {
            messagingService.send(msg.getReplyToQueue(), ackMsg); // Send ACK to primary's queue
        } catch (IOException e) {
            System.err.println("Failed to send REPLICATE_ACK: " + e.getMessage());
        }
    }

    private void handleReplicateAck(DSMMessage msg) {
        decrementPendingReplications(msg.getAddress());
    }

    private void forwardMessage(DSMMessage msg) {
        List<String> group = partitionConfig.getReplicationGroup(msg.getAddress());
        String targetNode;
        if (msg.getType() == DSMMessage.Type.READ) {
            if (group.size() > 1) {
                List<String> replicas = group.subList(1, group.size());
                targetNode = replicas.get(new Random().nextInt(replicas.size()));
            } else {
                // Fallback: if there's only one node, it’s effectively acting as both primary and replica
                targetNode = group.get(0);
            }
        } else {
            targetNode = group.get(0);
        }
        try {
            messagingService.send(targetNode, msg);
        } catch (IOException e) {
            System.err.println("Failed to forward message to " + targetNode + ": " + e.getMessage());
        }
    }


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

