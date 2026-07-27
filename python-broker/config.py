import os

# Load .env configuration programmatically
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(env_path):
    try:
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key.strip()] = val.strip()
        print("Successfully loaded environment configuration from .env")
    except Exception as e:
        print(f"Error loading .env file: {e}")

# Database Path
DB_PATH = os.path.join(os.getcwd(), "price_history.db")
