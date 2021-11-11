package org.broker.marketdata.client;

import org.broker.marketdata.configuration.ConfigurationFiles;

public class TestConfigurationFiles implements ConfigurationFiles {

  private String appFile;

  public void setAppFile(String appFile) {
    this.appFile = appFile;
  }

  @Override
  public String getAppConfigurationFile() {
    return appFile;
  }

  @Override
  public String getDockerConfigurationFile() {
    return null;
  }
}
