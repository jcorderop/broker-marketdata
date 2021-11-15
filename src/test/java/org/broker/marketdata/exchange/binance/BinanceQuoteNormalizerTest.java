package org.broker.marketdata.exchange.binance;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.vertx.core.json.JsonObject;
import org.broker.marketdata.protos.Quote;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest(properties="springBootApp.workOffline=true")
@ActiveProfiles("test")
class BinanceQuoteNormalizerTest {

  @Autowired
  BinanceConfig binanceConfig;

  @BeforeEach
  void setUp() {
  }

  @AfterEach
  void tearDown() {
  }

  private static JsonObject getPriceJson(String symbol, String bidPrice, String askPrice) {
    final JsonObject message = new JsonObject();
    message.put("s", symbol);
    message.put("b", bidPrice);
    message.put("a", askPrice);
    System.out.println("INPUT: " + message.encode());
    /*
    {
      "u":400900217,     // order book updateId
      "s":"BNBUSDT",     // symbol
      "b":"25.35190000", // best bid price
      "B":"31.21000000", // best bid qty
      "a":"25.36520000", // best ask price
      "A":"40.66000000"  // best ask qty
    }
    */
    return message;
  }

  @ParameterizedTest(name = "symbol={2},bidPrice={4},askPrice={6}")
  @CsvFileSource(resources = "/org/broker/marketdata/client/protos/normalizer/correctBinancePrices.csv", numLinesToSkip = 1)
  void create_a_complet_quote_from_bitmex_message_converted_to_internal_quote(String symbol
    , String bidPrice
    , String askPrice) {

    //given
    final JsonObject message = getPriceJson(symbol, bidPrice, askPrice);

    // when
    final Supplier<Stream<Quote>> streamSupplier = getQuoteSupplier(message);

    // then
    assertThat(streamSupplier.get().count()).isEqualTo(1);
    streamSupplier.get().forEach(quote -> {
      try {
        System.out.println(JsonFormat.printer().includingDefaultValueFields().print(quote));
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }

      assertThat(quote.getSource())
        .isEqualTo(binanceConfig.getSource());
      assertThat(quote.getTopic())
        .isEqualTo(BinanceConfig.JSON_TOPIC_INSTRUMENT);
      assertThat(quote.getAction())
        .isEqualTo(BinanceConfig.JSON_ACTION_UPDATE);
      assertThat(quote.getSymbol())
        .isEqualTo(symbol);
      assertThat(quote.hasMarkPrice()).isFalse();
      assertThat(quote.getBidPrice())
        .isEqualTo(Double.valueOf(Optional.ofNullable(bidPrice)
          .orElse("0.0")));
      assertThat(quote.hasMidPrice()).isTrue();
      assertThat(quote.getAskPrice())
        .isEqualTo(Double.valueOf(Optional.ofNullable(askPrice)
          .orElse("0.0")));
      assertThat(quote.hasVolume()).isFalse();
      assertThat(quote.getSourceTimestamp())
        .isNotNull();
      assertThat(quote.hasSourceTimestamp()).isEqualTo(true);
      assertThat(quote.hasArrivalTimestamp()).isEqualTo(true);

    });

  }

  @ParameterizedTest(name = "symbol={2},bidPrice={4},askPrice={6}")
  @CsvFileSource(resources = "/org/broker/marketdata/client/protos/normalizer/invalidBinancePrices.csv", numLinesToSkip = 1)
  void invalid_quote_from_bitmex_message_converted_to_internal_quote(String symbol
    , String bidPrice
    , String askPrice) {

    //given
    final JsonObject message = getPriceJson(symbol, bidPrice, askPrice);

    // when
    final Supplier<Stream<Quote>> streamSupplier = getQuoteSupplier(message);

    // then
    assertThat(streamSupplier.get().count()).isEqualTo(0);
    streamSupplier.get().forEach(quote -> {
      try {
        System.out.println(JsonFormat.printer().includingDefaultValueFields().print(quote));
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
      }
    });
  }

  private Supplier<Stream<Quote>> getQuoteSupplier(JsonObject message) {
    final List<Quote> quotes = new BinanceQuoteNormalizer(binanceConfig).stringToQuote(message.encode());
    return quotes::stream;
  }

}
