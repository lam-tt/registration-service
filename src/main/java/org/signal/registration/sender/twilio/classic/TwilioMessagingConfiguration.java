/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.registration.sender.twilio.classic;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Context;
import java.time.Duration;
import java.util.List;
import javax.validation.constraints.NotBlank;

@Context
@ConfigurationProperties("twilio.messaging")
class TwilioMessagingConfiguration {

  @NotBlank
  private String nanpaMessagingServiceSid;

  @NotBlank
  private String globalMessagingServiceSid;

  private List<@NotBlank String> supportedLanguages;

  private Duration sessionTtl = Duration.ofMinutes(10);

  public String getNanpaMessagingServiceSid() {
    return nanpaMessagingServiceSid;
  }

  public void setNanpaMessagingServiceSid(final String nanpaMessagingServiceSid) {
    this.nanpaMessagingServiceSid = nanpaMessagingServiceSid;
  }

  public String getGlobalMessagingServiceSid() {
    return globalMessagingServiceSid;
  }

  public void setGlobalMessagingServiceSid(final String globalMessagingServiceSid) {
    this.globalMessagingServiceSid = globalMessagingServiceSid;
  }

  public List<String> getSupportedLanguages() {
    return supportedLanguages;
  }

  public void setSupportedLanguages(final List<String> supportedLanguages) {
    this.supportedLanguages = supportedLanguages;
  }

  public Duration getSessionTtl() {
    return sessionTtl;
  }

  public void setSessionTtl(final Duration sessionTtl) {
    this.sessionTtl = sessionTtl;
  }
}
