import sqlite3
import os
import string
import random

# Number of Shards 
NUM_SHARDS = 5

# Shard DB Names
SHARD_NAMES = [f"shard{i}.db" for i in range(NUM_SHARDS)]

# Number of users to insert
NUM_USERS = 100

# Remove shard files if they already exist in filesystem
def remove_shards():
	for name in SHARD_NAMES:
		if os.path.exists(name):
			os.remove(name)

# Setup shards
def setup_shards():
	for name in SHARD_NAMES:
		conn = sqlite3.connect(name)
		cursor = conn.cursor()
		query = 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);'
		cursor.execute(query)
		conn.commit()
		conn.close()

# Function to get shard
def get_shard(user_id):
	shard = user_id % NUM_SHARDS
	return SHARD_NAMES[shard]

# Function to insert users:
def insert_users(user_id, name):
	shard_name = get_shard(user_id)
	conn = sqlite3.connect(shard_name)
	cursor = conn.cursor()
	cursor.execute('''INSERT INTO users (id, name) VALUES (?, ?)''', (user_id, name))
	conn.commit()
	conn.close()

# Function to get user
def get_user(user_id):
	shard_name = get_shard(user_id)
	conn = sqlite3.connect(shard_name)
	cursor = conn.cursor()
	cursor.execute('SELECT name FROM users WHERE id=?', (user_id,))
	result = cursor.fetchone()
	conn.close()
	return None if result is None else result[0]

# Function to generate random user names
def random_name():
	length = 6
	letters = string.ascii_lowercase
	return ''.join(random.choice(letters) for i in range(length))

def main():

	# Clean up existing shards
	remove_shards()
	# Set up new shards
	setup_shards()

	# Insert users
	for i in range(1, NUM_USERS+1):
		insert_users(i, random_name())

	# Get some users randomly
	n = 5
	for i in range(n):
		random_id = random.randint(1, NUM_USERS+1)
		user_name = get_user(random_id)
		print(f'User ID {random_id}, Name {user_name}')


main()
