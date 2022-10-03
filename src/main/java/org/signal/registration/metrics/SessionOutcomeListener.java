/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.registration.metrics;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import io.micrometer.core.instrument.Metrics;
import io.micronaut.configuration.metrics.annotation.RequiresMetrics;
import io.micronaut.context.event.ApplicationEventListener;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.signal.registration.session.RegistrationSession;
import org.signal.registration.session.SessionCompletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A session outcome listener reports basic metrics about completed sessions.
 */
@Singleton
@RequiresMetrics
public class SessionOutcomeListener implements ApplicationEventListener<SessionCompletedEvent> {

  private static final String COUNTER_NAME =
      MetricsUtil.name(SessionOutcomeListener.class, "completedSessions");

  private static final Logger logger = LoggerFactory.getLogger(SessionOutcomeListener.class);

  @Override
  public void onApplicationEvent(final SessionCompletedEvent event) {
    final RegistrationSession session = event.session();

    try {
      final Phonenumber.PhoneNumber phoneNumber =
          PhoneNumberUtil.getInstance().parse(event.session().getPhoneNumber(), null);

      Metrics.counter(COUNTER_NAME,
              "sender", session.getSenderName(),
              "verified", String.valueOf(StringUtils.isNotBlank(session.getVerifiedCode())),
              "countryCode", String.valueOf(phoneNumber.getCountryCode()),
              "regionCode", PhoneNumberUtil.getInstance().getRegionCodeForNumber(phoneNumber))
          .increment();
    } catch (final NumberParseException e) {
      logger.warn("Failed to parse phone number from completed session", e);
    }
  }
}
