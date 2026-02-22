# Login on Render (eko-fitness.onrender.com)

## Why "Invalid username or not approved"?

Render uses **its own database** (empty or seed data). The members you created on your Mac are in your **local** DuckDB, so they don't exist on Render.

---

## Option 1: Test with Admin (no DB needed)

**Admin login does not use the database.** It uses env vars (or defaults).

1. Open **https://eko-fitness.onrender.com** (or your local app pointing at it).
2. Go to the **Admin** page (e.g. `/admin` or use the admin login link if you have one).
3. Log in with:
   - **Username:** `admin`
   - **Password:** `admin123`

(If you set `ADMIN_USERNAME` and `ADMIN_PASSWORD` in Render's Environment, use those instead.)

You can then approve sign-ups, manage matchdays, etc. on the live API.

---

## Option 2: Use MotherDuck (same data as your Mac)

To have the **same members** on Render as on your Mac:

1. **Create a MotherDuck account** and get an API token: https://motherduck.com/
2. **Run the app locally once with MotherDuck:**
   - In `.env` (or environment): `USE_LOCAL_DB=false`, `MOTHERDUCK_TOKEN=your_token`
   - Start the backend so it creates the FOOTBALL schema in MotherDuck.
3. **Copy your local data into MotherDuck** (export local DuckDB tables, import into MotherDuck), or run signup/approve flows once so MotherDuck has the same users.
4. **On Render:** add Environment variables:
   - `USE_LOCAL_DB` = `false`
   - `MOTHERDUCK_TOKEN` = your MotherDuck token
5. Redeploy. Render will use the same cloud DB and your members will exist.

---

## Option 3: Sign up and approve on Render

1. On the **Member** side, use **Sign up** to create a new account (against the Render API).
2. Go to **Admin** and log in with `admin` / `admin123`.
3. In Admin, approve the pending sign-up.
4. Log in as that member (use the password from the approval email, or the generated one if you have it).
