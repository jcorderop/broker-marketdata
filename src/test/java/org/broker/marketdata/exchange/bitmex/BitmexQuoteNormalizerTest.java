package org.broker.marketdata.exchange.bitmex;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.vertx.core.json.JsonArray;
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
class BitmexQuoteNormalizerTest {

  @Autowired
  BitmexConfig bitmexConfig;

  @BeforeEach
  void setUp() {
  }

  @AfterEach
  void tearDown() {
  }

  private static JsonObject getPriceJson(String topic, String action, String symbol, Double markPrice, Double bidPrice, Double midPrice, Double askPrice, Double volume, String sourceTimestamp) {
    final JsonObject message = new JsonObject();
    message.put("table", topic);
    message.put("action", action);

    final JsonObject tick = new JsonObject();
    tick.put("symbol", symbol);
    tick.put("markPrice", markPrice);
    tick.put("bidPrice", bidPrice);
    tick.put("midPrice", midPrice);
    tick.put("askPrice", askPrice);
    tick.put("volume", volume);
    tick.put("timestamp", sourceTimestamp);
    message.put("data", new JsonArray().add(tick));
    System.out.println("INPUT: " + message.encode());
    return message;
  }

  @ParameterizedTest(name = "topic={0},action={1},symbol={2},markPrice={3},bidPrice={4}, midPrice={5},askPrice={6},volume={7},sourceTimestamp={8}")
  @CsvFileSource(resources = "/org/broker/marketdata/client/protos/normalizer/correctBitmexPrices.csv", numLinesToSkip = 1)
  void create_a_complet_quote_from_bitmex_message_converted_to_internal_quote(String topic
    , String action
    , String symbol
    , Double markPrice
    , Double bidPrice
    , Double midPrice
    , Double askPrice
    , Double volume
    , String sourceTimestamp) {

    //given
    final JsonObject message = getPriceJson(topic, action, symbol, markPrice, bidPrice, midPrice, askPrice, volume, sourceTimestamp);

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
        .isEqualTo(bitmexConfig.getSource());
      assertThat(quote.getTopic())
        .isEqualTo(topic);
      assertThat(quote.getAction())
        .isEqualTo(action);
      assertThat(quote.getSymbol())
        .isEqualTo(symbol);
      assertThat(quote.getMarkPrice())
        .isEqualTo(Optional.ofNullable(markPrice)
          .orElse(0.0));
      assertThat(quote.getBidPrice())
        .isEqualTo(Optional.ofNullable(bidPrice)
          .orElse(0.0));
      assertThat(quote.getMidPrice())
        .isEqualTo(Optional.ofNullable(midPrice)
          .orElse(0.0));
      assertThat(quote.getAskPrice())
        .isEqualTo(Optional.ofNullable(askPrice)
          .orElse(0.0));
      assertThat(quote.getVolume())
        .isEqualTo(Optional.ofNullable(volume)
          .orElse(0.0));
      assertThat(quote.getSourceTimestamp())
        .isNotNull();
      assertThat(quote.hasSourceTimestamp()).isEqualTo(true);
      assertThat(quote.hasArrivalTimestamp()).isEqualTo(true);

    });

  }

  @ParameterizedTest(name = "topic={0},action={1},symbol={2},markPrice={3},bidPrice={4}, midPrice={5},askPrice={6},volume={7},sourceTimestamp={8}")
  @CsvFileSource(resources = "/org/broker/marketdata/client/protos/normalizer/invalidBitmexPrices.csv", numLinesToSkip = 1)
  void invalid_quote_from_bitmex_message_converted_to_internal_quote(String topic
    , String action
    , String symbol
    , Double markPrice
    , Double bidPrice
    , Double midPrice
    , Double askPrice
    , Double volume
    , String sourceTimestamp) {

    //given
    final JsonObject message = getPriceJson(topic, action, symbol, markPrice, bidPrice, midPrice, askPrice, volume, sourceTimestamp);

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
    final List<Quote> quotes = new BitmexQuoteNormalizer(bitmexConfig).stringToQuote(message.encode());
    return quotes::stream;
  }

}
