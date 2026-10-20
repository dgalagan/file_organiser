import os

# --- Project root ---
try:
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
except NameError:
    PROJECT_ROOT = os.getcwd()

# --- Reference ---
REF_DIR = "ref"
REF_EXTENSION_MAP = "ext_mapping.csv"
REF_DIR_PATH = os.path.join(PROJECT_ROOT, REF_DIR)
EXTENSION_MAP_PATH = os.path.join(REF_DIR_PATH, REF_EXTENSION_MAP)

# --- Cache ---
CACHE_DIR = "cache"
CACHE_METADATA = "metadata.json"
CACHE_REGISTER = "register.json"
CACHE_DIR_PATH = os.path.join(PROJECT_ROOT, CACHE_DIR)
METADATA_PATH = os.path.join(CACHE_DIR_PATH, CACHE_METADATA)
REGISTER_PATH = os.path.join(CACHE_DIR_PATH, CACHE_REGISTER)

# --- Dir Schema ---
DIR_SCHEMA = "dir_schema.json"
DIR_SCHEMA_PATH = os.path.join(PROJECT_ROOT, DIR_SCHEMA)

# --- Output ---
OUTPUT_DIR = "output"
OUTPUT_DIR_PATH = os.path.join(PROJECT_ROOT, OUTPUT_DIR)