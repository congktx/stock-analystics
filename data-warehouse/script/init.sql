CREATE TABLE dim_companies (
    company_id             BIGINT NOT NULL PRIMARY KEY,
    company_name           NVARCHAR(255),
    company_ticker         NVARCHAR(32) NOT NULL,
    company_security_type  NVARCHAR(64),
    company_composite_figi NVARCHAR(32),
    company_cik            NVARCHAR(32),
    company_industry_name  NVARCHAR(255),
    company_sic            NVARCHAR(16)
);

CREATE TABLE dim_time (
    time_id          BIGINT NOT NULL PRIMARY KEY,
    time_date        DATE NOT NULL,
    time_day_of_week TINYINT,
    time_month       TINYINT,
    time_quarter     TINYINT,
    time_year        SMALLINT
);

CREATE TABLE dim_topics (
    topic_id   BIGINT NOT NULL PRIMARY KEY,
    topic_name NVARCHAR(255) NOT NULL
);

CREATE TABLE dim_news (
    news_id                     BIGINT NOT NULL PRIMARY KEY,
    news_time_id                BIGINT NOT NULL ,
    overall_score               FLOAT,
    news_title                  NVARCHAR(MAX),
    news_summary                NVARCHAR(MAX),
    news_category_within_score  FLOAT,
    news_source                 NVARCHAR(255),
    news_time_published         DATETIME2
);

CREATE TABLE fact_candles (
    candle_company_id      BIGINT NOT NULL ,
    candle_time_id         BIGINT NOT NULL ,

    candle_volume          FLOAT,
    candle_volume_weighted FLOAT,
    candle_open            FLOAT,
    candle_close           FLOAT,
    candle_high            FLOAT,
    candle_low             FLOAT,
    candle_num_of_trades   INT,

    candle_upper_wick      FLOAT,
    candle_lower_wick      FLOAT,
    candle_body_size       FLOAT,
    candle_is_bullish      BIT,
    candle_typical_price   FLOAT,
    candle_avg_trade_size  FLOAT
);

CREATE TABLE fact_news_companies (
    news_company_company_id BIGINT NOT NULL ,
    news_company_news_id    BIGINT NOT NULL ,

    news_company_sentiment_score FLOAT,
    news_company_relevance_score FLOAT
);

CREATE TABLE fact_news_topic (
    news_topic_news_id  BIGINT NOT NULL ,
    news_topic_topic_id BIGINT NOT NULL ,

    news_topic_relevant_score FLOAT
);

DROP TABLE IF EXISTS fact_news_topic;
DROP TABLE IF EXISTS fact_news_companies;
DROP TABLE IF EXISTS fact_candles;
DROP TABLE IF EXISTS dim_news;
DROP TABLE IF EXISTS dim_topics;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_companies;