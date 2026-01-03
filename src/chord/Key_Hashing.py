import pandas as pd
import hashlib
from bplustree import BPlusTree, BPlusTreeNode, printTree  # adjust import

# Load dataset
input_file_path = r"C:\Users\batma\DHT\data_movies_clean.xlsx"
df = pd.read_excel(input_file_path)

# Function to generate SHA-1 integer key from title
def get_sha1_key(title):
    sha1_hash = hashlib.sha1(title.encode('utf-8')).hexdigest()
    return int(sha1_hash, 16)

# Create the B+ Tree with leaf size = 3 (or whatever you want)
bplustree = BPlusTree(3)

# Insert first 5 rows as a test
for index, row in df.head(5).iterrows():
    key = get_sha1_key(row['title'])  # sortable key
    value = row.to_dict()             # full record
    bplustree.insert(value, key)      # value goes into leaf keys, key into values

result = bplustree.search_title("Explosion of a Motor Car")

print(result)