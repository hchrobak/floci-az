package io.floci.az.services.servicebus;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.impl.AckReason;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPStandardMessage;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ServiceBusDeadLetterMetadataTest {
    private static final SimpleString COUNT = SimpleString.of("x-opt-floci-dead-letter-delivery-count");

    @ParameterizedTest
    @ValueSource(strings = {"peeklock", "session"})
    void exhaustedDeliveryPreservesCountOnTheCopyOnly(String mode) throws Exception {
        AMQPStandardMessage original = message();
        Message copy = original.copy();
        MessageReference reference = reference(mode, 10);
        when(reference.getMessage()).thenReturn(original);

        apply(reference, copy, AckReason.KILLED, 10);

        assertEquals(10, copy.getBrokerProperty(COUNT));
        assertEquals("MaxDeliveryCountExceeded", copy.getStringProperty("DeadLetterReason"));
        assertNull(original.getBrokerProperty(COUNT));
        assertNull(original.getObjectProperty("DeadLetterReason"));
    }

    @Test
    void explicitDeadLetterKeepsItsReason() throws Exception {
        Message copy = message();
        copy.putStringProperty("DeadLetterReason", "BadPayload");
        apply(reference("peeklock", 10), copy, AckReason.KILLED, 10);
        assertEquals("BadPayload", copy.getStringProperty("DeadLetterReason"));
        assertNull(copy.getObjectProperty("DeadLetterErrorDescription"));
    }

    @Test
    void explicitDeadLetterBeforeTheLimitDoesNotClaimExhaustion() throws Exception {
        Message copy = message();
        apply(reference("peeklock", 1), copy, AckReason.KILLED, 10);
        assertEquals(1, copy.getBrokerProperty(COUNT));
        assertNull(copy.getObjectProperty("DeadLetterReason"));
    }

    @Test
    void otherQueuesAndExpiryDoNotAcquireDeliveryFailureMetadata() throws Exception {
        Message copy = message();
        MessageReference reference = reference("peeklock", 10);
        apply(reference, copy, AckReason.EXPIRED, 10);
        when(reference.getQueue().getUser()).thenReturn(null);
        apply(reference, copy, AckReason.KILLED, 10);
        assertNull(copy.getBrokerProperty(COUNT));
        assertNull(copy.getObjectProperty("DeadLetterReason"));
    }

    private static AMQPStandardMessage message() {
        return AMQPStandardMessage.createMessage(0, 0, null, null, null, null, null,
                Map.of("business-property", "preserved"), null, new AmqpValue("payload"));
    }

    private static MessageReference reference(String mode, int count) {
        MessageReference reference = mock(MessageReference.class, RETURNS_DEEP_STUBS);
        when(reference.getQueue().getUser()).thenReturn(SimpleString.of("floci-az:servicebus-" + mode + ":60"));
        when(reference.getDeliveryCount()).thenReturn(count);
        return reference;
    }

    private static void apply(MessageReference reference, Message copy, AckReason reason, int limit) throws Exception {
        try (URLClassLoader loader = new URLClassLoader(new URL[]{
                ServiceBusDeadLetterMetadataTest.class.getResource(ArtemisPatchVersions.ARTEMIS_SERVER_PATCH_RESOURCE)},
                ServiceBusDeadLetterMetadataTest.class.getClassLoader())) {
            Class.forName("org.apache.activemq.artemis.core.server.impl.ServiceBusDeadLetterMetadata", true, loader)
                    .getMethod("apply", MessageReference.class, Message.class, AckReason.class, int.class)
                    .invoke(null, reference, copy, reason, limit);
        }
    }
}
