package org.broker.marketdata.configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.validation.constraints.NotBlank;
import java.util.Map;

@Getter
@Setter
@ToString
public abstract class AbstractExchangeConfig implements ExchangeConfig {
  @NotBlank
  protected String source;
  @NotBlank
  protected String host;
  @NotBlank
  protected Integer port;
  @NotBlank
  protected String path;

  protected Map<String, String> symbol;

  @NotBlank
  protected Integer maxWebSocketFrameSize;
  @NotBlank
  protected Integer maxWebSocketMessageSize;
}
