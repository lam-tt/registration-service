package org.signal.registration.analytics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.signal.registration.rpc.ClientType;
import org.signal.registration.rpc.MessageTransport;
import org.signal.registration.util.UUIDUtil;
import reactor.core.publisher.Flux;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MemoryAttemptPendingAnalysisRepositoryTest {

	private MemoryAttemptPendingAnalysisRepository repository;

	@BeforeEach
	void setUp() {
		repository = new MemoryAttemptPendingAnalysisRepository(new SimpleMeterRegistry());
	}

	@Test
	void storeAndGetByRemoteIdentifier() {
		final AttemptPendingAnalysis attemptPendingAnalysis = buildAttemptPendingAnalysis("test");

		assertEquals(Optional.empty(),
			repository.getByRemoteIdentifier(attemptPendingAnalysis.getSenderName(), attemptPendingAnalysis.getRemoteId()).join());

		repository.store(attemptPendingAnalysis).join();

		assertEquals(Optional.of(attemptPendingAnalysis),
			repository.getByRemoteIdentifier(attemptPendingAnalysis.getSenderName(), attemptPendingAnalysis.getRemoteId()).join());
	}

	@Test
	void storeDuplicateEvent() {
		final AttemptPendingAnalysis attemptPendingAnalysis = buildAttemptPendingAnalysis("test");

		assertDoesNotThrow(() -> repository.store(attemptPendingAnalysis).join());
		assertDoesNotThrow(() -> repository.store(attemptPendingAnalysis).join());

		assertEquals(Optional.of(attemptPendingAnalysis),
			repository.getByRemoteIdentifier(attemptPendingAnalysis.getSenderName(), attemptPendingAnalysis.getRemoteId()).join());
	}

	@Test
	void getBySender() {
		final String sender = "first";
		final Set<AttemptPendingAnalysis> expectedAttemptsPendingAnalysis = new HashSet<>();

		for (int i = 0; i < 10; i++) {
			final AttemptPendingAnalysis attemptPendingAnalysis = buildAttemptPendingAnalysis(sender);

			repository.store(attemptPendingAnalysis).join();
			expectedAttemptsPendingAnalysis.add(attemptPendingAnalysis);
		}

		for (int i = 0; i < 10; i++) {
			repository.store(buildAttemptPendingAnalysis(sender + "-unexpected")).join();
		}

		assertEquals(expectedAttemptsPendingAnalysis,
			Flux.from(repository.getBySender(sender)).collect(Collectors.toSet()).block());
	}

	@Test
	void remove() {
		assertDoesNotThrow(() -> repository.remove("does-not-exist", "does-not-exist"));

		final AttemptPendingAnalysis removedAttempt = buildAttemptPendingAnalysis("test");
		final AttemptPendingAnalysis remainingAttempt = buildAttemptPendingAnalysis("test");

		repository.store(removedAttempt).join();
		repository.store(remainingAttempt).join();

		assertEquals(Optional.of(removedAttempt),
			repository.getByRemoteIdentifier(removedAttempt.getSenderName(), removedAttempt.getRemoteId()).join());

		repository.remove(removedAttempt.getSenderName(), removedAttempt.getRemoteId()).join();

		assertEquals(Optional.empty(),
			repository.getByRemoteIdentifier(removedAttempt.getSenderName(), removedAttempt.getRemoteId()).join());

		assertEquals(Optional.of(remainingAttempt),
			repository.getByRemoteIdentifier(remainingAttempt.getSenderName(), remainingAttempt.getRemoteId()).join());
	}

	private static AttemptPendingAnalysis buildAttemptPendingAnalysis(final String senderName) {
		return AttemptPendingAnalysis.newBuilder()
			.setSessionId(UUIDUtil.uuidToByteString(UUID.randomUUID()))
			.setAttemptId(ThreadLocalRandom.current().nextInt(0, 100))
			.setSenderName(senderName)
			.setRemoteId(RandomStringUtils.randomAlphanumeric(16))
			.setMessageTransport(ThreadLocalRandom.current().nextBoolean() ? MessageTransport.MESSAGE_TRANSPORT_SMS : MessageTransport.MESSAGE_TRANSPORT_VOICE)
			.setClientType(ClientType.CLIENT_TYPE_UNSPECIFIED)
			.setRegion(RandomStringUtils.randomAlphabetic(2))
			.setTimestampEpochMillis(System.currentTimeMillis())
			.setAccountExistsWithE164(ThreadLocalRandom.current().nextBoolean())
			.setVerified(ThreadLocalRandom.current().nextBoolean())
			.build();
	}
}
