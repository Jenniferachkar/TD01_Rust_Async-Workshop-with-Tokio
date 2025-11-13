# TD01_Rust_Async-Workshop-with-Tokio & TD_02_Rust_WebSocket_Workshop_with_Tokio
### by Jennifer El Achkar

## Overview
This project is part of my Rust programming class (TD01) at ESILV.
The goal was to build a stock price aggregator that fetches prices from multiple APIs asynchronously using the Tokio runtime.
The final result is a small yet complete application that connects to real stock APIs, saves the prices every minute, and can shut down gracefully when needed.

## Features
Multiple APIs (Alpha Vantage, Finnhub, and a mock)
Full async design using Tokio
Data persistence with PostgreSQL
Safe error handling: it keeps running even when one API fails
Graceful shutdown (using Ctrl + C)
Clear structured logs

## Project Structure
```css
TD1_2_Rust_Tokio/
│
├── TD1/
│   ├── rust-1.md      
│
├── TD2/
│   ├── rust-2.md      
│   ├── src/
│   │   ├── main.rs    
│   ├── Cargo.toml     

```

## How to run the project
### 1. Clone the repository
```bash
cd TD1_2_Rust_Tokio
git clone https://github.com/Jenniferachkar/TD1_2_Rust_Tokio
```
### 2. Set up the database
```bash
createdb stockdb
psql -U postgres -d stockdb -f schema.sql
```
### 3. I Created a '.env' file at the root project
```ini
DATABASE_URL=postgresql://postgres:22042004@localhost/stockdb
ALPHA_VANTAGE_KEY=8HWOYWB98QVMO2D9     # from: https://www.alphavantage.co/support/#api-key
FINNHUB_KEY=d4abmmhr01qnehvtrulgd4abmmhr01qnehvtrum0   # from: https://finnhub.io/dashboard
```

### 4. run the program
```bash
cargo run
```
Once launched, the program will:
  Fetch stock prices every minute
  Store results in PostgreSQL
  Display structured logs in the terminal
Press Ctrl + C to stop it safely.
