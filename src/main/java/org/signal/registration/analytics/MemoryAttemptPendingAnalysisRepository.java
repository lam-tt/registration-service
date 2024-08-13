package org.signal.registration.analytics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import jakarta.inject.Singleton;
import org.reactivestreams.Publisher;
import org.signal.registration.Environments;
import org.signal.registration.metrics.MetricsUtil;
import org.signal.registration.util.ReactiveResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@Requires(env = {Environments.DEVELOPMENT, Environment.TEST})
@Requires(missingBeans = AttemptPendingAnalysisRepository.class)
public class MemoryAttemptPendingAnalysisRepository implements AttemptPendingAnalysisRepository {
	private final MeterRegistry meterRegistry;

	private final Map<String, ArrayList<AttemptPendingAnalysis>> attemptsBySender = new ConcurrentHashMap<>();

	private static final String STORE_ATTEMPT_COUNTER_NAME =
		MetricsUtil.name(MemoryAttemptPendingAnalysisRepository.class, "store");

	private static final String GET_ATTEMPT_BY_REMOTE_IDENTIFIER_COUNTER_NAME =
		MetricsUtil.name(MemoryAttemptPendingAnalysisRepository.class, "getByRemoteId");

	private static final String GET_ATTEMPTS_BY_SENDER_COUNTER_NAME =
		MetricsUtil.name(MemoryAttemptPendingAnalysisRepository.class, "getBySender");

	private static final String REMOVE_ATTEMPT_COUNTER_NAME =
		MetricsUtil.name(MemoryAttemptPendingAnalysisRepository.class, "remove");

	private static final String SENDER_TAG_NAME = "sender";

	private static final Logger logger = LoggerFactory.getLogger(MemoryAttemptPendingAnalysisRepository.class);

	public MemoryAttemptPendingAnalysisRepository(final MeterRegistry meterRegistry) {
		this.meterRegistry = meterRegistry;
	}

	@Override
	public CompletableFuture<Void> store(final AttemptPendingAnalysis attemptPendingAnalysis) {
		String senderName = attemptPendingAnalysis.getSenderName();
		var list = attemptsBySender.get(senderName);
		if ( list == null) {
			list = new ArrayList<AttemptPendingAnalysis>();
			attemptsBySender.put(senderName, list);
		}
		list.add(attemptPendingAnalysis);
		meterRegistry.counter(STORE_ATTEMPT_COUNTER_NAME, SENDER_TAG_NAME, senderName).increment();
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public CompletableFuture<Optional<AttemptPendingAnalysis>> getByRemoteIdentifier(final String senderName,
		final String remoteId) {
		Optional<AttemptPendingAnalysis> result = Optional.empty();

		var list = attemptsBySender.get(senderName);
		if (list == null) {
			logger.warn("getRemoteIdentifier failed: attempt pending not found");
		} else {
			result = list.stream()
				.filter(it -> remoteId == it.getRemoteId())
				.findFirst();
		}

		meterRegistry.counter(GET_ATTEMPT_BY_REMOTE_IDENTIFIER_COUNTER_NAME,
				SENDER_TAG_NAME, senderName)
			.increment();

		return CompletableFuture.completedFuture(result);
	}

	@Override
	public Publisher<AttemptPendingAnalysis> getBySender(final String senderName) {
		return ReactiveResponseObserver.<AttemptPendingAnalysis>asFlux(observer -> {
			var list = attemptsBySender.get(senderName);
			if (list != null) {
				list.forEach(it -> observer.onResponse(it));
			}
			observer.onComplete();
		}).doOnNext(result ->
			meterRegistry.counter(GET_ATTEMPTS_BY_SENDER_COUNTER_NAME, SENDER_TAG_NAME, senderName).increment()
		);
	}

	@Override
	public CompletableFuture<Void> remove(final String senderName, final String remoteId) {
		var list = attemptsBySender.get(senderName);
		if (list == null) {
			logger.warn("remove failed: attempt pending not found");
		} else {
			list.removeIf(it -> remoteId == it.getRemoteId());
		}

		meterRegistry.counter(REMOVE_ATTEMPT_COUNTER_NAME, SENDER_TAG_NAME, senderName).increment();
		return CompletableFuture.completedFuture(null);
	}
}
