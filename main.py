from input_handling import get_user_input
import sys
import traceback

def main():
    try:
        files = get_user_input()
        return 0
    except Exception:
        print("--- CRASH REPORT ---")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())