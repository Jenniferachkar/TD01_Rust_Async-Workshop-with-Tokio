# TD01_Rust_Async-Workshop-with-Tokio
### by Jennifer El Achkar

---
## English Version  
*(Pour la version française à voir plus bas)*
---

---
## Overview
This project is part of my Rust programming class (TD01) at ESILV.
The goal was to build a stock price aggregator that fetches prices from multiple APIs asynchronously using the Tokio runtime.
The final result is a small yet complete application that connects to real stock APIs, saves the prices every minute, and can shut down gracefully when needed.
---

---
## Features
Multiple APIs (Alpha Vantage, Finnhub, and a mock)
Full async design using Tokio
Data persistence with PostgreSQL
Safe error handling: it keeps running even when one API fails
Graceful shutdown (using Ctrl + C)
Clear structured logs
---

---
## Project Structure
```css
├── Cargo.toml          # Project dependencies
├── schema.sql          # Database schema (table creation)
├── src/
│   └── main.rs         # Main Rust source code
└── .gitignore
```
---

---
## How to run the project
### 1. Clone the repository
```bash
git clone https://github.com/Jenniferachkar/TD01_Rust_Async-Workshop-with-Tokio.git
cd TD01_Rust_Async-Workshop-with-Tokio
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

---

---
## Version Française
---

---
## Présentation
Ce projet a été réalisé dans le cadre du cours de programmation en Rust (TD01) à l’ESILV.
L’objectif était de créer un agrégateur de prix d’actions capable de récupérer des données depuis plusieurs APIs de manière asynchrone, en utilisant Tokio, la bibliothèque asynchrone principale de Rust.
Le résultat final est une application simple mais complète, capable de se connecter à de vraies APIs, d’enregistrer les prix toutes les minutes dans une base PostgreSQL, et de s’arrêter proprement à tout moment.
---

---
## Fonctionnalités
Récupération de données depuis plusieurs APIs (Alpha Vantage, Finnhub, et une source simulée)
Design entièrement asynchrone grâce à Tokio
Stockage des données dans PostgreSQL
Gestion des erreurs robuste (le programme continue même si une API échoue)
Arrêt propre (Ctrl + C)
Logs clairs et structurés
---

---
## Structure du Projet
```css
├── Cargo.toml         
├── schema.sql        
├── src/
│   └── main.rs      
└── .gitignore
```
---

---
## Comment Exécuter le Projet
### 1. Cloner le Dépôt
```bash
git clone https://github.com/Jenniferachkar/TD01_Rust_Async-Workshop-with-Tokio.git
cd TD01_Rust_Async-Workshop-with-Tokio
```
### 2.Configurer la Base de Données
```bash
createdb stockdb
psql -U postgres -d stockdb -f schema.sql
```
### 3. Créer un Fichier .env à la Racine du Projet
```ini
DATABASE_URL=postgresql://postgres:22042004@localhost/stockdb
ALPHA_VANTAGE_KEY=8HWOYWB98QVMO2D9     # from: https://www.alphavantage.co/support/#api-key
FINNHUB_KEY=d4abmmhr01qnehvtrulgd4abmmhr01qnehvtrum0   # from: https://finnhub.io/dashboard
```

### 4. Lancer le Programme
```bash
cargo run
```
Once launched, the program will:
  Fetch stock prices every minute
  Store results in PostgreSQL
  Display structured logs in the terminal
Press Ctrl + C to stop it safely.

---

