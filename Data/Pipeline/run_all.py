from load_raw import main as load_raw
from build_silver import main as build_silver
from build_gold import main as build_gold

if __name__ == "__main__":
    load_raw()
    build_silver()
    build_gold()
    print("✅ Pipeline complete: raw → silver → gold")