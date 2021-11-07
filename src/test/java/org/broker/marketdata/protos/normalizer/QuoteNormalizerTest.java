package org.broker.marketdata.protos.normalizer;


import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.broker.marketdata.protos.normalizer.QuoteNormalizer.stringToQuotos;
import static org.junit.jupiter.api.Assertions.*;

class QuoteNormalizerTest {

  @BeforeEach
  void setUp() {
  }

  @AfterEach
  void tearDown() {
  }

  @Test
  void create_a_quote_from_bitmex_message() {
    String message = "{\n" +
      "  \"table\": \"instrument\",\n" +
      "  \"action\": \"update\",\n" +
      "  \"data\": [\n" +
      "    {\n" +
      "      \"symbol\": \"XBTUSD\",\n" +
      "      \"fairPrice\": 61544.95,\n" +
      "      \"fairBasis\": 18.98,\n" +
      "      \"markPrice\": 61544.95,\n" +
      "      \"openValue\": 93721006815,\n" +
      "      \"timestamp\": \"2021-11-06T21:47:50.000Z\",\n" +
      "      \"indicativeSettlePrice\": 61525.97\n" +
      "    }\n" +
      "  ]\n" +
      "}";
    stringToQuotos(message).stream().forEach(quote -> {
      try {
        String json = JsonFormat.printer().print(quote);
        System.out.println(json);
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }
      //final DocumentContext messageContext = JsonPath.parse(quote).;
      //System.out.println(messageContext.jsonString());
      //JsonObject json = JsonObject.mapFrom(quote.getDefaultInstanceForType());
      //System.out.println(json);
    });


  }

}
