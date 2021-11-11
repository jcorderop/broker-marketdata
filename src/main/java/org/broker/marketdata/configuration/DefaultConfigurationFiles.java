package org.broker.marketdata.configuration;

public class DefaultConfigurationFiles implements ConfigurationFiles {

  @Override
  public String getAppConfigurationFile() {
    return "application.yaml";
  }

  @Override
  public String getDockerConfigurationFile() {
    return "application_docker.yaml";
  }
}
