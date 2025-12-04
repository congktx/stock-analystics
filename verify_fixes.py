import os
import sys

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "internal-database/src"))

from utils.config import GlobalConfig
from utils.utils import _init_env

def verify():
    print("Verifying fixes...")
    
    # 1. Check Config defaults
    print(f"DB_HOST: {GlobalConfig.DB_HOST}")
    print(f"API_URL: {GlobalConfig.API_URL}")
    
    # 2. Check Path Resolution
    _init_env()
    print(f"ROOT: {GlobalConfig.ROOT}")
    print(f"MARKET_STATUS_PATH: {GlobalConfig.MARKET_STATUS_PATH}")
    
    # Check if ROOT is absolute
    if os.path.isabs(GlobalConfig.ROOT):
        print("PASS: ROOT is absolute")
    else:
        print("FAIL: ROOT is not absolute")
        
    # Check if ROOT ends with 'internal-database' (based on my assumption in utils.py)
    # Actually, based on my code in utils.py:
    # current_dir = .../src/utils
    # src_dir = .../src
    # project_root = .../internal-database
    
    if GlobalConfig.ROOT.endswith("internal-database"):
        print("PASS: ROOT seems correct")
    else:
        print(f"WARN: ROOT might be incorrect: {GlobalConfig.ROOT}")

if __name__ == "__main__":
    verify()
