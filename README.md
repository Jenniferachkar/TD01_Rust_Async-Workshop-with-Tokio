# TD01_Rust_Async-Workshop-with-Tokio & TD_02_Rust_WebSocket_Workshop_with_Tokio
### by Jennifer El Achkar ESILV A4

## Overview
This repository contains two complete Rust workshops built with Tokio, covering:

TD1: Async Programming & Database Integration
TD2: WebSockets, Broadcasting & Real-Time Dashboard

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
│   ├── Cargo.toml
│   ├── rust-1.md          
│   └── src/
│       └── main.rs        
│
├── TD2/
│   ├── Cargo.toml
│   ├── rust-2.md          
│   └── src/
│       └── main.rs        
│
├── schema.sql             
├── README.md              
└── Cargo.toml             
```

## How to run the project
### 1. Clone the repository
```bash
cd TD1_2_Rust_Tokio
git clone https://github.com/Jenniferachkar/TD1_2_Rust_Tokio
cargo check
cargo build
```
### 2. Set up the database
```bash
createdb stockdb
psql -U postgres -d stockdb -f schema.sql
```
### 3. I Created a '.env' file at the root project
```ini
ALPHA_VANTAGE_KEY=your_api_key
FINNHUB_KEY=your_api_key
DATABASE_URL=postgresql://user:password@localhost/stockdb
```
### 4. Run the TD1
```bash
cd TD1
cargo run
```
Once launched, the program will:
  Fetch stock prices every minute
  Store results in PostgreSQL
  Display structured logs in the terminal
Press Ctrl + C to stop it safely.

### 5. Run the TD2
```bash
cd TD2
cargo run
```
Open the included HTML file: dashboard.html
