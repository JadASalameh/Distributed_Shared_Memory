import java.io.IOException;
import java.util.function.Consumer;

public interface MessagingService {
    void send(String queue, DSMMessage msg) throws IOException;
    void sendReply(String replyQueue, String value) throws IOException;
    void startMessageListener(String queueName, Consumer<DSMMessage> handler) throws IOException;
}

