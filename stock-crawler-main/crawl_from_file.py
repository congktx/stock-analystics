import json
import time
import os
from datetime import datetime
from service import get_ohlc, load_all_ohlc_to_db, get_news_sentiment, load_all_news_sentiment_to_db
from database.mongodb import MongoDB
from utils.network_utils import toggle_cloudflare_warp, get_current_ip, check_warp_status

def get_checkpoint_path():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, 'checkpoint.json')

def save_checkpoint(checkpoint_data):
    try:
        with open(get_checkpoint_path(), 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    except Exception as e:
        print(f"Lỗi khi lưu checkpoint: {e}")

def load_checkpoint():
    try:
        checkpoint_path = get_checkpoint_path()
        if os.path.exists(checkpoint_path):
            with open(checkpoint_path, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Lỗi khi đọc checkpoint: {e}")
    return None

def clear_checkpoint():
    try:
        checkpoint_path = get_checkpoint_path()
        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)
            print("Đã xóa checkpoint cũ")
    except Exception as e:
        print(f"Lỗi khi xóa checkpoint: {e}")

def load_ticker_list(file_path='tencty_2.json'):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(script_dir, file_path)
        
        with open(full_path, 'r') as f:
            content = f.read()
            if not content.strip().startswith('{'):
                content = '{' + content + '}'
            data = json.loads(content)
            # Lấy list ticker từ key "Chien"
            if 'Chien' in data:
                tickers = data['Chien']
                print(f"Đã load {len(tickers)} ticker từ file {full_path}")
                return tickers
            else:
                print(f"Không tìm thấy key 'Chien' trong file")
                return []
    except Exception as e:
        print(f"Lỗi khi đọc file: {e}")
        return []

def crawl_ohlc_for_tickers(tickers, from_timestamp, to_timestamp, time_update, start_index=0):
    """
    Args:
        tickers: List ticker symbols
        from_timestamp: Timestamp bắt đầu
        to_timestamp: Timestamp kết thúc
        time_update: Timestamp cập nhật
        start_index: Index bắt đầu
    """
    mongodb = MongoDB()
    total = len(tickers)
    success_count = 0
    error_count = 0
    
    print("\n" + "="*60)
    if start_index > 0:
        print(f"TIẾP TỤC CRAWL OHLC DATA TỪ TICKER #{start_index + 1}")
    else:
        print(f"BẮT ĐẦU CRAWL OHLC DATA CHO {total} TICKER")
    print("="*60 + "\n")
    
    for idx in range(start_index, total):
        ticker = tickers[idx]
        try:
            print(f"[{idx + 1}/{total}] Crawling {ticker}...", end=" ")
            
            result = get_ohlc(ticker, from_timestamp, to_timestamp)
            
            if result is None or not isinstance(result, tuple):
                error_count += 1
                print(f"No data or invalid response")
                time.sleep(12)
                continue
            
            list_ohlc, next_url = result
            
            if list_ohlc and not isinstance(list_ohlc, str):
                load_all_ohlc_to_db(ticker, list_ohlc, time_update)
                
                # Xử lý pagination nếu có
                page_count = 1
                while next_url:
                    page_count += 1
                    from service.ohlc_crawler import ohlc_get_next_url
                    next_result = ohlc_get_next_url(next_url)
                    if next_result and isinstance(next_result, tuple):
                        list_ohlc, next_url = next_result
                        if list_ohlc and not isinstance(list_ohlc, str):
                            load_all_ohlc_to_db(ticker, list_ohlc, time_update)
                    else:
                        break
                    time.sleep(12)  # Rate limiting
                
                success_count += 1
                print(f"Done ({page_count} page(s))")
            else:
                error_count += 1
                print(f"No data or error: {list_ohlc}")
            
            time.sleep(12)
            
            if (idx + 1) % 10 == 0:
                checkpoint_data = {
                    'last_index': idx,
                    'last_ticker': ticker,
                    'success_count': success_count,
                    'error_count': error_count,
                    'timestamp': datetime.now().isoformat(),
                    'type': 'ohlc'
                }
                save_checkpoint(checkpoint_data)
            
        except KeyboardInterrupt:
            # Lưu checkpoint
            checkpoint_data = {
                'last_index': idx,
                'last_ticker': ticker,
                'success_count': success_count,
                'error_count': error_count,
                'timestamp': datetime.now().isoformat(),
                'type': 'ohlc'
            }
            save_checkpoint(checkpoint_data)
            print(f"Đã lưu checkpoint tại ticker #{idx + 1}: {ticker}")
            raise
        except Exception as e:
            error_count += 1
            print(f"Error: {e}")
            time.sleep(12)
    
    print("\n" + "="*60)
    print(f"HOÀN TẤT CRAWL OHLC")
    print(f"   - Thành công: {success_count}/{total}")
    print(f"   - Lỗi: {error_count}/{total}")
    print("="*60 + "\n")
     
    clear_checkpoint()

def crawl_news_for_tickers(tickers, from_timestamp, to_timestamp, time_update, start_index=0, auto_change_ip=False, ip_change_interval=25):
    """
    Args:
        tickers: List ticker symbols
        from_timestamp: Timestamp bắt đầu
        to_timestamp: Timestamp kết thúc
        time_update: Timestamp cập nhật
        start_index: Index bắt đầu
        auto_change_ip: Tự động đổi IP sau mỗi interval ticker
        ip_change_interval: Số ticker crawl trước khi đổi IP
    """
    mongodb = MongoDB()
    total = len(tickers)
    success_count = 0
    error_count = 0
    requests_since_ip_change = 0
    
    print("\n" + "="*60)
    if start_index > 0:
        print(f"TIẾP TỤC CRAWL NEWS SENTIMENT TỪ TICKER #{start_index + 1}")
    else:
        print(f"BẮT ĐẦU CRAWL NEWS SENTIMENT CHO {total} TICKER")
    
    if auto_change_ip:
        print(f"Tự động đổi IP sau mỗi {ip_change_interval} ticker")
        current_ip = get_current_ip()
        if current_ip:
            print(f"IP hiện tại: {current_ip}")
    
    print("="*60 + "\n")
    
    for idx in range(start_index, total):
        ticker = tickers[idx]
        try:
            # Đổi IP 
            if auto_change_ip and requests_since_ip_change >= ip_change_interval:
                print(f"\nĐã crawl {ip_change_interval} ticker, đang đổi IP...")
                if toggle_cloudflare_warp():
                    requests_since_ip_change = 0
                    print("Đã đổi IP thành công, tiếp tục crawl...\n")
                    time.sleep(5)
                else:
                    print("Không thể đổi IP, tiếp tục với IP hiện tại...\n")
                    requests_since_ip_change = 0
            
            print(f"[{idx + 1}/{total}] Crawling news for {ticker}...", end=" ")
            
            # Get news từ API
            list_news = get_news_sentiment(ticker, from_timestamp, to_timestamp)
            requests_since_ip_change += 1
            
            if list_news:
                # Lưu vào MongoDB
                load_all_news_sentiment_to_db(list_news, time_update)
                success_count += 1
                print(f"{len(list_news)} news")
            else:
                error_count += 1
                print(f"No news")
            
            time.sleep(12)
            
            # Lưu checkpoint sau mỗi 10 ticker
            if (idx + 1) % 10 == 0:
                checkpoint_data = {
                    'last_index': idx,
                    'last_ticker': ticker,
                    'success_count': success_count,
                    'error_count': error_count,
                    'timestamp': datetime.now().isoformat(),
                    'type': 'news'
                }
                save_checkpoint(checkpoint_data)
            
        except KeyboardInterrupt:
            # Lưu checkpoint 
            checkpoint_data = {
                'last_index': idx,
                'last_ticker': ticker,
                'success_count': success_count,
                'error_count': error_count,
                'timestamp': datetime.now().isoformat(),
                'type': 'news'
            }
            save_checkpoint(checkpoint_data)
            print(f"Đã lưu checkpoint tại ticker #{idx + 1}: {ticker}")
            raise
        except Exception as e:
            error_count += 1
            print(f"Error: {e}")
            time.sleep(12)
    
    print("\n" + "="*60)
    print(f"HOÀN TẤT CRAWL NEWS")
    print(f"   - Thành công: {success_count}/{total}")
    print(f"   - Lỗi: {error_count}/{total}")
    print("="*60 + "\n")
    
    # Xóa checkpoint
    clear_checkpoint()

def main():
    print("  STOCK CRAWLER - DANH SÁCH TICKER TỪ FILE")
    
    # Load ticker
    tickers = load_ticker_list('tencty_2.json')
    
    if not tickers:
        print("Không có ticker nào để crawl!")
        return
    
    print(f"Tổng số ticker: {len(tickers)}\n")
    
    # Kiểm tra checkpoint
    checkpoint = load_checkpoint()
    start_index = 0
    
    if checkpoint:
        print(f"Tìm thấy checkpoint:")
        print(f"   - Đã crawl đến: Ticker #{checkpoint['last_index'] + 1} ({checkpoint['last_ticker']})")
        print(f"   - Thành công: {checkpoint.get('success_count', 0)}")
        print(f"   - Lỗi: {checkpoint.get('error_count', 0)}")
        print(f"   - Thời gian: {checkpoint.get('timestamp', 'N/A')}")
        
        resume = input(f"\n Tiếp tục từ ticker #{checkpoint['last_index'] + 2}? (y/n): ").strip().lower()
        if resume == 'y':
            start_index = checkpoint['last_index'] + 1
        else:
            clear_checkpoint()
    
    print("\nChọn loại dữ liệu muốn crawl:")
    print("  1. OHLC Data (giá cổ phiếu)")
    print("  2. News Sentiment (tin tức)")
    print("  3. Cả hai")
    
    choice = input("\nNhập lựa chọn (1/2/3): ").strip()
    
    # Crawl 2 năm trở lại đây: từ 1/11/2023 đến 9/11/2025
    from_timestamp = int(datetime(2023, 11, 1).timestamp())
    to_timestamp = int(datetime(2025, 11, 9).timestamp())
    time_update = int(datetime.now().timestamp())
    
    print(f"\nThời gian crawl:")
    print(f"   - From: {datetime.fromtimestamp(from_timestamp).strftime('%Y-%m-%d')}")
    print(f"   - To: {datetime.fromtimestamp(to_timestamp).strftime('%Y-%m-%d')}")
    
    auto_change_ip = False
    if choice in ['2', '3']:
        # Kiểm tra WARP status
        warp_status = check_warp_status()
        if warp_status == "not_installed":
            print("\nCloudflare WARP chưa được cài đặt")
            print("Tải tại: https://1.1.1.1/")
        else:
            print(f"\nCloudflare WARP status: {warp_status}")
        
        change_ip_input = input("Tự động đổi IP (qua WARP) sau mỗi 25 ticker? (y/n): ").strip().lower()
        if change_ip_input == 'y':
            if warp_status != "not_installed":
                auto_change_ip = True
                print("Sẽ tự động đổi IP qua Cloudflare WARP")
            else:
                print("Vui lòng cài đặt WARP trước!")
                return
    
    if start_index == 0:
        confirm = input("\nBắt đầu crawl? (y/n): ").strip().lower()
    else:
        confirm = input(f"\nTiếp tục crawl từ ticker #{start_index + 1}? (y/n): ").strip().lower()
    
    if confirm != 'y':
        print("Đã hủy!")
        return
    
    start_time = time.time()
    
    # Crawl theo lựa chọn
    if choice == '1':
        crawl_ohlc_for_tickers(tickers, from_timestamp, to_timestamp, time_update, start_index)
    elif choice == '2':
        crawl_news_for_tickers(tickers, from_timestamp, to_timestamp, time_update, start_index, auto_change_ip)
    elif choice == '3':
        crawl_ohlc_for_tickers(tickers, from_timestamp, to_timestamp, time_update, start_index)
        crawl_news_for_tickers(tickers, from_timestamp, to_timestamp, time_update, start_index, auto_change_ip)
    else:
        print("Lựa chọn không hợp lệ!")
        return
    
    elapsed_time = time.time() - start_time
    hours = int(elapsed_time // 3600)
    minutes = int((elapsed_time % 3600) // 60)
    seconds = int(elapsed_time % 60)
    
    print(f"  HOÀN TẤT TẤT CẢ!")
    print(f"  Thời gian: {hours}h {minutes}m {seconds}s")

if __name__ == '__main__':
    main()
