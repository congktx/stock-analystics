-- SQL Server Database Schema for Stock News Analytics
-- Database: StockAnalytics

-- Create database if not exists
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'StockAnalytics')
BEGIN
    CREATE DATABASE StockAnalytics;
END
GO

USE StockAnalytics;
GO

-- Table 1: news_sentiment_processed
-- Stores processed news articles with sentiment analysis
IF OBJECT_ID('news_sentiment_processed', 'U') IS NULL
BEGIN
    CREATE TABLE news_sentiment_processed (
        id BIGINT PRIMARY KEY IDENTITY(1,1),
        message_id VARCHAR(100) UNIQUE NOT NULL,
        source_id VARCHAR(100) NOT NULL,
        title NVARCHAR(500) NOT NULL,
        url NVARCHAR(1000),
        time_published DATETIME2 NOT NULL,
        summary NVARCHAR(MAX),
        source_name VARCHAR(100),
        overall_sentiment_score DECIMAL(5,4),
        overall_sentiment_label VARCHAR(20),
        processed_at DATETIME2 DEFAULT GETUTCDATE(),
        created_at DATETIME2 DEFAULT GETUTCDATE()
    );
    
    -- Indexes for query performance
    CREATE INDEX idx_time_published ON news_sentiment_processed(time_published);
    CREATE INDEX idx_source ON news_sentiment_processed(source_name);
    CREATE INDEX idx_sentiment ON news_sentiment_processed(overall_sentiment_label);
    CREATE INDEX idx_message_id ON news_sentiment_processed(message_id);
    
    PRINT 'Table news_sentiment_processed created successfully';
END
GO

-- Table 2: news_ticker_mapping
-- Maps news articles to relevant ticker symbols with sentiment scores
IF OBJECT_ID('news_ticker_mapping', 'U') IS NULL
BEGIN
    CREATE TABLE news_ticker_mapping (
        id BIGINT PRIMARY KEY IDENTITY(1,1),
        news_id BIGINT NOT NULL,
        ticker VARCHAR(10) NOT NULL,
        relevance_score DECIMAL(5,4),
        ticker_sentiment_score DECIMAL(5,4),
        ticker_sentiment_label VARCHAR(20),
        created_at DATETIME2 DEFAULT GETUTCDATE(),
        
        -- Foreign key
        CONSTRAINT FK_news_ticker_news FOREIGN KEY (news_id) 
            REFERENCES news_sentiment_processed(id) ON DELETE CASCADE
    );
    
    -- Indexes
    CREATE INDEX idx_ticker ON news_ticker_mapping(ticker);
    CREATE INDEX idx_news_id ON news_ticker_mapping(news_id);
    CREATE INDEX idx_ticker_sentiment ON news_ticker_mapping(ticker, ticker_sentiment_label);
    
    PRINT 'Table news_ticker_mapping created successfully';
END
GO

-- Table 3: ticker_sentiment_daily
-- Daily aggregated sentiment statistics per ticker
IF OBJECT_ID('ticker_sentiment_daily', 'U') IS NULL
BEGIN
    CREATE TABLE ticker_sentiment_daily (
        id BIGINT PRIMARY KEY IDENTITY(1,1),
        ticker VARCHAR(10) NOT NULL,
        date DATE NOT NULL,
        news_count INT NOT NULL,
        avg_sentiment_score DECIMAL(5,4),
        bullish_count INT DEFAULT 0,
        bearish_count INT DEFAULT 0,
        neutral_count INT DEFAULT 0,
        top_sentiment_score DECIMAL(5,4),
        created_at DATETIME2 DEFAULT GETUTCDATE(),
        updated_at DATETIME2 DEFAULT GETUTCDATE(),
        
        -- Unique constraint
        CONSTRAINT UQ_ticker_date UNIQUE (ticker, date)
    );
    
    -- Indexes
    CREATE INDEX idx_ticker_date ON ticker_sentiment_daily(ticker, date);
    CREATE INDEX idx_date ON ticker_sentiment_daily(date);
    CREATE INDEX idx_ticker ON ticker_sentiment_daily(ticker);
    
    PRINT 'Table ticker_sentiment_daily created successfully';
END
GO

-- Sample Queries

-- Query 1: Get latest news for a ticker
/*
SELECT TOP 10
    nsp.title,
    nsp.time_published,
    nsp.source_name,
    nsp.overall_sentiment_score,
    nsp.overall_sentiment_label,
    ntm.ticker_sentiment_score,
    ntm.ticker_sentiment_label,
    nsp.url
FROM news_sentiment_processed nsp
JOIN news_ticker_mapping ntm ON nsp.id = ntm.news_id
WHERE ntm.ticker = 'AAPL'
ORDER BY nsp.time_published DESC;
*/

-- Query 2: Get daily sentiment summary for a ticker
/*
SELECT 
    ticker,
    date,
    news_count,
    avg_sentiment_score,
    bullish_count,
    bearish_count,
    neutral_count,
    CAST(bullish_count AS FLOAT) / NULLIF(news_count, 0) * 100 as bullish_pct
FROM ticker_sentiment_daily
WHERE ticker = 'AAPL'
    AND date >= DATEADD(day, -30, GETDATE())
ORDER BY date DESC;
*/

-- Query 3: Top tickers by news volume today
/*
SELECT TOP 10
    ticker,
    news_count,
    avg_sentiment_score,
    bullish_count,
    bearish_count
FROM ticker_sentiment_daily
WHERE date = CAST(GETDATE() AS DATE)
ORDER BY news_count DESC;
*/

-- Query 4: Sentiment distribution for a ticker
/*
SELECT 
    ntm.ticker_sentiment_label,
    COUNT(*) as count,
    AVG(ntm.ticker_sentiment_score) as avg_score
FROM news_ticker_mapping ntm
JOIN news_sentiment_processed nsp ON ntm.news_id = nsp.id
WHERE ntm.ticker = 'AAPL'
    AND nsp.time_published >= DATEADD(day, -7, GETDATE())
GROUP BY ntm.ticker_sentiment_label
ORDER BY count DESC;
*/

-- Query 5: Most mentioned tickers in last 24 hours
/*
SELECT TOP 20
    ntm.ticker,
    COUNT(*) as mention_count,
    AVG(ntm.ticker_sentiment_score) as avg_sentiment,
    SUM(CASE WHEN ntm.ticker_sentiment_label = 'Bullish' THEN 1 ELSE 0 END) as bullish,
    SUM(CASE WHEN ntm.ticker_sentiment_label = 'Bearish' THEN 1 ELSE 0 END) as bearish
FROM news_ticker_mapping ntm
JOIN news_sentiment_processed nsp ON ntm.news_id = nsp.id
WHERE nsp.time_published >= DATEADD(hour, -24, GETDATE())
GROUP BY ntm.ticker
ORDER BY mention_count DESC;
*/

PRINT 'Schema creation completed successfully!';
GO
