from __future__ import annotations

import argparse
import csv
import datetime
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass 

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# API Configuration
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
API_BASE = "https://www.alphavantage.co/query"
RATE_LIMIT_MINUTE = 5  # API calls per minute (free tier)
RATE_LIMIT_DAY = 500   # API calls per day (free tier)

@dataclass
class AlphaVantageConfig:
    """Configuration for Alpha Vantage API access."""
    api_key: str
    output_dir: Path
    symbols: List[str]
    functions: List[str]
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class RateLimiter:
    """Handles API rate limiting."""
    def __init__(self, calls_per_minute: int, calls_per_day: int):
        self.calls_per_minute = calls_per_minute
        self.calls_per_day = calls_per_day
        self.minute_calls = []
        self.daily_calls = []
    
    def wait_if_needed(self):
        """Wait if we're approaching rate limits."""
        now = time.time()
        
        # Clean old timestamps
        minute_ago = now - 60
        day_ago = now - 86400
        self.minute_calls = [t for t in self.minute_calls if t > minute_ago]
        self.daily_calls = [t for t in self.daily_calls if t > day_ago]
        
        # Check rate limits
        if len(self.minute_calls) >= self.calls_per_minute:
            sleep_time = 61 - (now - self.minute_calls[0])
            logger.info(f"Rate limit approaching, sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)
        
        if len(self.daily_calls) >= self.calls_per_day:
            raise Exception("Daily API call limit reached")
        
        # Record this call
        self.minute_calls.append(now)
        self.daily_calls.append(now)


class AlphaVantageCrawler:
    """Handles crawling financial data from Alpha Vantage."""
    
    def __init__(self, config: AlphaVantageConfig):
        if not config.api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required")
        
        self.config = config
        self.rate_limiter = RateLimiter(RATE_LIMIT_MINUTE, RATE_LIMIT_DAY)
        self.session = requests.Session()
    
    def fetch_data(self, symbol: str, function: str) -> Dict[str, Any]:
        """Fetch data for a symbol and function from Alpha Vantage API."""
        self.rate_limiter.wait_if_needed()
        
        params = {
            "function": function,
            "symbol": symbol,
            "apikey": self.config.api_key,
        }
        
        if function == "TIME_SERIES_DAILY":
            params["outputsize"] = "full"
        
        try:
            response = self.session.get(API_BASE, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if "Error Message" in data:
                raise Exception(f"API Error: {data['Error Message']}")
            if "Note" in data:
                raise Exception(f"API Limit: {data['Note']}")
                
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {symbol} {function}: {e}")
            raise
    
    def save_data(self, symbol: str, function: str, data: Dict[str, Any]):
        """Save API response data to JSON and CSV files."""
        # Create output directory
        output_dir = self.config.output_dir / symbol
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save JSON
        json_path = output_dir / f"{function.lower()}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        
        # Save CSV (if time series data)
        if function == "TIME_SERIES_DAILY":
            csv_path = output_dir / f"{function.lower()}.csv"
            time_series = data.get("Time Series (Daily)", {})
            if time_series:
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["date", "open", "high", "low", "close", "volume"])
                    for date, values in time_series.items():
                        writer.writerow([
                            date,
                            values["1. open"],
                            values["2. high"],
                            values["3. low"],
                            values["4. close"],
                            values["5. volume"],
                        ])
    
    def crawl(self):
        """Crawl all configured symbols and functions."""
        for symbol in self.config.symbols:
            logger.info(f"Processing symbol: {symbol}")
            for function in self.config.functions:
                try:
                    logger.info(f"Fetching {function} data...")
                    data = self.fetch_data(symbol, function)
                    self.save_data(symbol, function, data)
                    logger.info(f"Saved {function} data for {symbol}")
                except Exception as e:
                    logger.error(f"Failed to process {symbol} {function}: {e}")
                    continue


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Crawl financial data from Alpha Vantage API")
    parser.add_argument("--symbols", required=True, help="Comma-separated list of stock symbols")
    parser.add_argument(
        "--functions",
        default="TIME_SERIES_DAILY,OVERVIEW,INCOME_STATEMENT,BALANCE_SHEET,CASH_FLOW",
        help="Comma-separated list of API functions to fetch",
    )
    parser.add_argument("--output-dir", default="alpha_vantage_data", help="Output directory for data files")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD) for time series data")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD) for time series data")
    args = parser.parse_args(argv)
    
    try:
        if not API_KEY:
            logger.error("ALPHA_VANTAGE_API_KEY environment variable is required")
            return 1
            
        config = AlphaVantageConfig(
            api_key=API_KEY,  # type: ignore # we checked it's not None above
            output_dir=Path(args.output_dir),
            symbols=args.symbols.split(","),
            functions=args.functions.split(","),
            start_date=args.start_date,
            end_date=args.end_date,
        )
        
        crawler = AlphaVantageCrawler(config)
        crawler.crawl()
        
        return 0
        
    except Exception as e:
        logger.error(f"Crawler failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
