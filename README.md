# 📚 SQL Server + SQLPad Labs

A lightweight SQL Server 2022 environment with SQLPad (web UI) for running teaching labs. Labs live under `lectures/day_*` and are executed inside the SQL Server container.

---

## 🚀 Getting Started

### Prerequisites

Make sure you have the following installed:

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)
- `make`

### Setup

1. Create a `.env` with sensible defaults:

   ```sh
   make setup
   ```

   Then edit `.env` if desired:

   - `SA_PASSWORD`: strong password for SQL Server (required)
   - `DB_NAME`: target database name (default: `PracticeDB`)
   - `SQLPAD_ADMIN` / `SQLPAD_ADMIN_PASSWORD`: SQLPad admin credentials

2. Start services:

   ```sh
   make up
   ```

   This starts SQL Server (Developer) on `localhost:1433` and SQLPad at `http://localhost:3000`.

3. Run a lab (example: Day 8):

   ```sh
   make day-run DAY=8
   ```

   This executes all SQL files in `lectures/day_8/sql` inside the container.

4. Connect via SQLPad (optional):

   - Open `http://localhost:3000` and log in with `SQLPAD_ADMIN` / `SQLPAD_ADMIN_PASSWORD`.
   - A connection to your `DB_NAME` is pre-provisioned.

5. Connect via sqlcmd (optional):

   ```sh
   docker exec -it mssql /bin/bash -lc 'if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT DB_NAME()" -d "$DB_NAME"; else /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -Q "SELECT DB_NAME()" -d "$DB_NAME"; fi'
   ```

---

## 📂 Project Structure

- `docker/`: Docker Compose for SQL Server and SQLPad
- `lectures/day_8/`: Data warehousing lab
  - `data/`: CSVs mounted to the container at `/opt/workspace/lectures/day_8/data`
  - `sql/`: Executable SQL scripts
  - `queries/`: Example queries (see `queries/practical.sql`)
- `scripts/`: Helper scripts invoked by `make`

---

## 🛠️ Makefile Commands

- `make help`: List available commands
- `make setup`: Create `.env` if missing
- `make up`: Start SQL Server and SQLPad
- `make down`: Stop services
- `make clean`: Remove containers and volumes
- `make day-run DAY=N`: Run all SQL files under `lectures/day_N/sql`

---

## 🔎 Notes & Troubleshooting

- The repo root is mounted to the container as `/opt/workspace` (see `docker/docker-compose.yml`).
- BULK INSERT paths in the SQL scripts point to this mount, e.g. `/opt/workspace/lectures/day_8/data/...`.
- CSV newlines are expected as `\n`. If you are on Windows, ensure no `\r\n` issues.
- For the events dataset (`day_8`): the CSV includes a leading `EventID` column with empty values so SQL Server auto-generates the identity. Do not remove this column.
- If you see `sqlcmd not found`, the script will try both `/opt/mssql-tools18/bin/sqlcmd` and `/opt/mssql-tools/bin/sqlcmd` inside the container.

Apple Silicon: these images run under x86_64 emulation; performance may vary.

References:

- SQL Server Linux containers quickstart: https://learn.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-ver17&tabs=cli

---

## 📄 License

MIT License.
