# Eko React App – Football

A **separate** React app for your football team (30 players).  
- **React app** = main DLT frontend (`frontend/`)  
- **Eko React app** = this app (`eko-react/`)

## Features

- **Sign up** – Players register with first name, surname, baller name, jersey (1–100), email, WhatsApp. Status: pending until approved.
- **Admin** – Approve/reject sign-ups. On approve, a **password** is generated and sent by email:
  - **Username** = baller name  
  - **Password** = `Eko-[letters in first name]-[first+last letter of baller name in caps]-[year]`  
  - Example: John, Flash, 2026 → `Eko-4-FH-2026`
- **Login** – Players use the username and password from the email.
- **Dashboard** – Simple landing after login (mobile-friendly).

## Backend

Uses the **same DLT FastAPI backend**. Football API is under `/api/v1/football`.  
Database: **DuckDB** schema `FOOTBALL`, table `FOOTBALL.players`. The schema is created automatically when the API starts.

## Setup

### 1. Backend (DLT API)

From the **DLT project root** (not eko-react):

```bash
# Optional: create FOOTBALL schema once (or let API create it on startup)
python scripts/init_football_schema.py

# Optional: install email validation for signup
pip install email-validator passlib[bcrypt]
```

**Environment variables** (e.g. in DLT `.env`):

- `GMAIL_USER` – Sender email (default: `leocasey0@gmail.com`).
- `GMAIL_APP_PASSWORD` – **Gmail App Password** (not your normal Gmail password).  
  - Go to Google Account → Security → 2-Step Verification → App passwords → generate one for “Mail”.  
  - Put that 16-character password in `GMAIL_APP_PASSWORD`.
- `ADMIN_USERNAME` – Admin login (default: `admin`).
- `ADMIN_PASSWORD` – Admin password (default: `admin123`). Change in production.

Start the API:

```bash
uvicorn main:app --reload --port 8000
```

### 2. Eko React app

```bash
cd eko-react
npm install
npm run dev
```

Runs at **http://localhost:5174**.  
Set `VITE_FOOTBALL_API_URL=http://localhost:8000/api/v1/football` if your API is on another host/port.

## Gmail

Use **App Password**, not your normal Gmail password:

1. Google Account → Security → 2-Step Verification (must be on).
2. App passwords → Generate for “Mail”.
3. Copy the 16-character password into `GMAIL_APP_PASSWORD` in your `.env`.

If you don’t set `GMAIL_APP_PASSWORD`, approve still works but no email is sent (logged only).

## Routes

| Path        | Description                |
|------------|----------------------------|
| `/signup`  | Player registration        |
| `/login`   | Player login               |
| `/dashboard` | Player dashboard (after login) |
| `/admin`   | Admin login + pending list, approve/reject |
