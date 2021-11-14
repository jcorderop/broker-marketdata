package org.broker.marketdata;

import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class SpringbootVertxApplication {

  @Autowired
  private MarketDataService marketDataService;

	public static void main(String[] args) {
		SpringApplication.run(SpringbootVertxApplication.class, args);
	}


  @Value("${springBootApp.workOffline:false}")
  private boolean workOffline = false;

  @PostConstruct
  public void deployVerticle() {
    if (!workOffline) {
      final Vertx vertx = Vertx.vertx();
      vertx.deployVerticle(marketDataService);
    }
  }

}
