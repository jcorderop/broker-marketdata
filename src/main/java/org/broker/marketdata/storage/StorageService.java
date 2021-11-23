package org.broker.marketdata.storage;

import lombok.AllArgsConstructor;
import org.broker.marketdata.protos.Quote;
import org.springframework.stereotype.Service;

@AllArgsConstructor
@Service
public class StorageService {

  private final QuoteRepository quoteRepository;

  public void insertNewQuote(final Quote quote) {
    quoteRepository.save(QuoteRowMapper.mapQuoteEntityFromQuoteProto(quote));
  }
}
