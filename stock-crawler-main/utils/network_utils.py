import subprocess
import time
import requests
import os

def get_current_ip():
    """Lấy IP hiện tại"""
    try:
        response = requests.get('https://api.ipify.org?format=json', timeout=10)
        return response.json().get('ip')
    except:
        return None

def toggle_cloudflare_warp():
    try:
        warp_path = r"C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe"
        
        # Kiểm tra WARP đã cài chưa
        if not os.path.exists(warp_path):
            print("   Cloudflare WARP chưa được cài đặt!")
            print("   Tải tại: https://1.1.1.1/")
            return False
        
        print(f"\n Đang đổi IP qua Cloudflare WARP...")
        old_ip = get_current_ip()
        if old_ip:
            print(f"    IP cũ: {old_ip}")
        
        # Disconnect WARP
        subprocess.run([warp_path, "disconnect"], 
                      capture_output=True, 
                      timeout=10)
        print("   Đã disconnect WARP")
        time.sleep(2)
        
        # Connect WARP
        subprocess.run([warp_path, "connect"], 
                      capture_output=True, 
                      timeout=10)
        print("  Đã connect WARP")
        time.sleep(5)
        
        # Kiểm tra IP mới
        new_ip = get_current_ip()
        if new_ip and new_ip != old_ip:
            print(f"  IP mới: {new_ip}")
            return True
        elif new_ip:
            print(f"   IP không đổi: {new_ip}")
            return False
        else:
            print("   Không lấy được IP")
            return False
            
    except subprocess.TimeoutExpired:
        print("   Timeout khi chạy WARP")
        return False
    except Exception as e:
        print(f"   Lỗi: {e}")
        return False

def check_warp_status():
    try:
        warp_path = r"C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe"
        if not os.path.exists(warp_path):
            return "not_installed"
        
        result = subprocess.run([warp_path, "status"], 
                              capture_output=True, 
                              text=True,
                              timeout=5)
        if "Connected" in result.stdout:
            return "connected"
        elif "Disconnected" in result.stdout:
            return "disconnected"
        else:
            return "unknown"
    except:
        return "error"

def restart_network_adapter(adapter_name="Wi-Fi"):
    try:
        print(f"\nĐang restart network adapter: {adapter_name}...")
        
        # Disable adapter
        subprocess.run(
            f'netsh interface set interface "{adapter_name}" admin=disable',
            shell=True,
            check=True,
            capture_output=True
        )
        print(" Đã tắt adapter")
        time.sleep(5)
        
        # Enable adapter
        subprocess.run(
            f'netsh interface set interface "{adapter_name}" admin=enable',
            shell=True,
            check=True,
            capture_output=True
        )
        print("  Đã bật lại adapter")
        time.sleep(10)  # Đợi kết nối lại
        
        # Kiểm tra IP mới
        new_ip = get_current_ip()
        if new_ip:
            print(f"   IP mới: {new_ip}")
            return True
        else:
            print("   Không lấy được IP mới")
            return False
            
    except subprocess.CalledProcessError as e:
        print(f"  Lỗi: {e}")
        print("  Chạy terminal với quyền Admin")
        return False
    except Exception as e:
        print(f"   Lỗi không xác định: {e}")
        return False

def get_network_adapters():
    try:
        result = subprocess.run(
            'netsh interface show interface',
            shell=True,
            capture_output=True,
            text=True
        )
        return result.stdout
    except:
        return None

if __name__ == "__main__":
    # Test
    print("Testing network utilities...")
    print(f"\nIP hiện tại: {get_current_ip()}")
    print(f"\nNetwork adapters:")
    print(get_network_adapters())
