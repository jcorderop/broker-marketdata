CREATE TABLE quotes
(
    id         SERIAL PRIMARY KEY,

    source VARCHAR,
    topic VARCHAR,
    symbol VARCHAR,
    action VARCHAR,

    mark_price NUMERIC,
    bid_price NUMERIC,
    mid_price NUMERIC,
    ask_price NUMERIC,
    volume NUMERIC,

    source_timestamp INTEGER,
    arrival_timestamp INTEGER,
    publish_timestamp INTEGER,

    CONSTRAINT mark_price_is_positive CHECK (mark_price > 0),
    CONSTRAINT bid_price_is_positive CHECK (bid_price > 0),
    CONSTRAINT mid_price_is_positive CHECK (mid_price > 0),
    CONSTRAINT ask_price_is_positive CHECK (ask_price > 0),
    CONSTRAINT volume_is_positive_or_zero CHECK (volume >= 0)
);
