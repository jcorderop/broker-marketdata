package org.broker.marketdata.configuration;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Getter @Setter
@ToString
public class ExchangeConfigImpl {

  private static final Logger logger = LoggerFactory.getLogger(ExchangeConfigImpl.class);

  @NonNull
  String source;
  @NonNull
  String host;
  @NonNull
  Integer port;
  @NonNull
  String path;

  List<String> symbol;

  @NonNull
  Integer maxWebSocketFrameSize;
  @NonNull
  Integer maxWebSocketMessageSize;
}
