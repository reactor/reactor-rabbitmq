package reactor.rabbitmq;

import java.util.HashMap;
import java.util.Map;

/**
 * Fluent API to specify creation of an exchange with delayed feature.
 * The <a href="https://github.com/rabbitmq/rabbitmq-delayed-message-exchange">delayed message plugin</a>
 * need to be enabled for RabbitMQ.
 */
public class DelayedExchangeSpecification extends ExchangeSpecification {

    private static final String DELAYED_TYPE = "x-delayed-message";
    private static final String DELAYED_ARGUMENT_KEY = "x-delayed-type";

    public DelayedExchangeSpecification() {
        type(super.getType());
    }

    public static ExchangeSpecification exchange() {
        return new DelayedExchangeSpecification();
    }

    public static ExchangeSpecification exchange(String name) {
        return new DelayedExchangeSpecification().name(name);
    }

    @Override
    public DelayedExchangeSpecification type(String type) {
        Map<String, Object> args = getArguments();
        if (args == null) {
            args = new HashMap<>();
        }
        args.put(DELAYED_ARGUMENT_KEY, type);
        arguments(args);
        return this;
    }

    @Override
    public String getType() {
        return DELAYED_TYPE;
    }

}
