# Assignment-4-ngangada

# 📦 Assignment 4: Data Fragmentation – CSE 511

## 🗂️ Overview

This project implements horizontal data fragmentation in PostgreSQL using Python. The objective is to simulate two partitioning strategies — **Range Partitioning** and **Round-Robin Partitioning** — on the `subreddits.csv` dataset based on the `created_utc` column.

The project was completed as part of **CSE 511: Data Processing at Scale - Spring 2025**.

---

## 📁 Files Included

- `assignment4.py` – Core logic for data loading and partitioning.
- `subreddits.header` – Contains column names and types for the `subreddits` table.
- `subreddits.csv` – Dataset used (not included here due to size/privacy).
- `test_load.py` – Example script to run and validate the implemented functions.

---

## ⚙️ Setup & Requirements

Make sure you have the following installed:

- Python 3.8+
- PostgreSQL 14
- Python packages:
  ```bash
  pip install pandas psycopg2
