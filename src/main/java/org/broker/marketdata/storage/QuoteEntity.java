package org.broker.marketdata.storage;

import lombok.*;

import javax.persistence.*;
import javax.validation.constraints.NotBlank;

@Builder
@ToString
@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "Quotes")
public class QuoteEntity {
  @Id
  @SequenceGenerator(
    name = "quote_sequence",
    sequenceName = "quote_sequence",
    allocationSize = 1
  )
  @GeneratedValue(
    generator = "quote_sequence",
    strategy = GenerationType.SEQUENCE)
  private Long id;

  @Column(nullable = false)
  Long quoteId;

  @NotBlank
  @Column(nullable = false)
  String source;
  @NotBlank
  @Column(nullable = false)
  String topic;
  @NotBlank
  @Column(nullable = false)
  String action;
  @NotBlank
  @Column(nullable = false)
  String stage;

  @NotBlank
  @Column(nullable = false)
  String symbol;

  Double markPrice;
  Double bidPrice;
  Double midPrice;
  Double askPrice;
  Double volume;

  @NonNull
  Long sourceTimestamp;
  @NonNull
  Long arrivalTimestamp;
  Long publishTimestamp;
}
