package org.broker.marketdata;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(properties = "springBootApp.workOffline=true")
@ActiveProfiles("test")
public class AbstractSpringBootTest {
}
