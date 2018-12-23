package reactor.rabbitmq;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.SignalType;

import java.util.function.BiConsumer;

public class ChannelCloseHandlers {

    public static class SenderChannelCloseHandler implements BiConsumer<SignalType, Channel> {

        private static final Logger LOGGER = LoggerFactory.getLogger(SenderChannelCloseHandler.class);

        @Override
        public void accept(SignalType signalType, Channel channel) {
            int channelNumber = channel.getChannelNumber();
            LOGGER.info("closing channel {} by signal {}", channelNumber, signalType);
            try {
                if (channel.isOpen() && channel.getConnection().isOpen()) {
                    channel.close();
                }
            } catch (Exception e) {
                LOGGER.warn("Channel {} didn't close normally: {}", channelNumber, e.getMessage());
            }
        }
    }

}
