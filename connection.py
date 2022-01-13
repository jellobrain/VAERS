#!/usr/bin/env python3
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import mysql.connector


# First configure the connection.
def connect(user, password):
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,  # 2433
        user=user,
        password=password,
        database='VAERS'
    )
    return conn


