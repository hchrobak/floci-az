package org.apache.activemq.artemis.core.server.impl;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.MessageReference;

/** Captures entity-local metadata on the dead-letter copy before it is routed or persisted. */
public final class ServiceBusDeadLetterMetadata {
    private static final SimpleString DELIVERY_COUNT = SimpleString.of("x-opt-floci-dead-letter-delivery-count");

    private ServiceBusDeadLetterMetadata() {
    }

    public static void apply(MessageReference reference, Message copy, AckReason reason, int maxDeliveries) {
        SimpleString user = reference.getQueue().getUser();
        if (reason != AckReason.KILLED || user == null
                || !(user.toString().startsWith("floci-az:servicebus-peeklock:")
                || user.toString().startsWith("floci-az:servicebus-session:"))) {
            return;
        }
        copy.setBrokerProperty(DELIVERY_COUNT, reference.getDeliveryCount());
        if (maxDeliveries > 0 && reference.getDeliveryCount() >= maxDeliveries
                && copy.getObjectProperty("DeadLetterReason") == null) {
            copy.putStringProperty("DeadLetterReason", "MaxDeliveryCountExceeded");
            copy.putStringProperty("DeadLetterErrorDescription", "Message could not be consumed after "
                    + maxDeliveries + " delivery attempts.");
        }
        copy.reencode();
    }
}
