package org.broker.marketdata.configuration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.validation.constraints.NotBlank;

@Configuration
@ConfigurationProperties(prefix = "ws.server")
@Getter
@Setter
@ToString
public class WebsocketServerConfig {
  @NotBlank
  private String host;
  @NotBlank
  private Integer port;
  @NotBlank
  private String path;
}
