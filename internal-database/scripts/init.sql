DROP SCHEMA datasource CASCADE;
CREATE SCHEMA datasource;

CREATE TABLE datasource.companies(
	company_ticker VARCHAR(50) PRIMARY KEY,
	company_cik VARCHAR(50),
	company_composite_figi VARCHAR(50),
	company_market_locale VARCHAR(50),
	company_share_class_figi VARCHAR(50),
	company_asset_type VARCHAR(50),
	company_name VARCHAR(255)
);

CREATE TABLE datasource.markets(
	market_region VARCHAR(50) PRIMARY KEY,
	market_type VARCHAR(50),
	market_local_close VARCHAR(50),
	market_local_open VARCHAR(50)
);

CREATE TABLE datasource.market_status(
	market_status_region VARCHAR(50) REFERENCES datasource.markets(market_region),
	market_status_time_update INT4 NOT NULL,
	market_status_current_status VARCHAR(50),
	PRIMARY KEY (market_status_region, market_status_time_update)
);



CREATE TABLE datasource.exchanges(
	exchange_mic VARCHAR(50) PRIMARY KEY,
	exchange_region VARCHAR(50) NOT NULL REFERENCES datasource.markets(market_region),
	exchange_name VARCHAR(50)
);

CREATE TABLE datasource.company_status(
	company_status_ticker VARCHAR(50) REFERENCES datasource.companies(company_ticker),
	company_status_primary_exchange VARCHAR(50) REFERENCES datasource.exchanges(exchange_mic),
	company_status_time_update INT4,
	company_status_type VARCHAR(50),
	company_status_active VARCHAR(50),
	PRIMARY KEY (company_status_ticker, company_status_time_update)
);

COPY datasource.companies(company_ticker,
							company_cik,
							company_composite_figi,
							company_market_locale,
							company_share_class_figi,
							company_asset_type,
							company_name)
FROM 'D:\work-space\stock-analysis\internal-database\data\csv\companies.csv'
DELIMITER ','
CSV HEADER;

COPY datasource.markets(market_region,
						market_type,
						market_local_close,
						market_local_open)
FROM 'D:\work-space\stock-analysis\internal-database\data\csv\markets.csv'
DELIMITER ','
CSV HEADER;

COPY datasource.market_status(market_status_region,
								market_status_time_update,
								market_status_current_status)
FROM 'D:\work-space\stock-analysis\internal-database\data\csv\market_status.csv'
DELIMITER ','
CSV HEADER;

COPY datasource.exchanges(exchange_mic,
							exchange_region,
							exchange_name)
FROM 'D:\work-space\stock-analysis\internal-database\data\csv\exchanges.csv'
DELIMITER ','
CSV HEADER;

COPY datasource.company_status(company_status_ticker,
								company_status_primary_exchange,
								company_status_time_update,
								company_status_type,
								company_status_active)
FROM 'D:\work-space\stock-analysis\internal-database\data\csv\company_status.csv'
DELIMITER ','
CSV HEADER;
