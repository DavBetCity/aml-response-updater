import os
import sqlite3
import psycopg2
from dotenv import load_dotenv


def connect_sqlite():
    conn = sqlite3.connect("db.sqlite3")

    return conn, conn.cursor()


def connect_postgres():
    load_dotenv()

    try:
        conn = psycopg2.connect(
            port=os.environ.get("DB_PORT"),
            host=os.environ.get("DB_HOST"),
            user=os.environ.get("DB_USER"),
            database=os.environ.get("DB_NAME"),
            password=os.environ.get("DB_PASSWORD"),
        )

        return conn, conn.cursor()

    except Exception as e:
        print(e)
        return None, None
