# BB Record - Bitbay Account Data Collector

This application collects wallet balance and coin greeks data from Bitbay API and stores it in a PostgreSQL database. It runs scheduled data collection every 5 minutes using APScheduler.

## Features

- **Wallet Balance Collection**: Fetches and stores account wallet balance data
- **Coin Greeks Collection**: Fetches and stores options greeks data
- **Scheduled Execution**: Automatically runs every 5 minutes
- **Database Storage**: Saves data to PostgreSQL database
- **Logging**: Outputs logs to stdout with timestamps

## Prerequisites

- Python 3.8+
- PostgreSQL database
- Bitbay API credentials

## Installation

1. Install dependencies:
```bash
pip install pybit python-dotenv psycopg2-binary apscheduler
```

2. Set up environment variables in `.env`:
```
API_BB_OPTION=your_api_key
API_BB_OPTION_KEY=your_api_secret
NEXUS_PG_HOST=localhost
NEXUS_PG_PORT=5432
NEXUS_PG_USER=postgres
NEXUS_PG_PASSWORD=your_password
NEXUS_PG_DATABASE=your_database
```

## Database Schema

The application creates two tables:

### wallet_balance
- `id`: Primary key
- `timestamp`: API timestamp
- `total_equity`: Total equity amount
- `account_im_rate`: Initial margin rate
- `total_margin_balance`: Total margin balance
- `total_initial_margin`: Total initial margin
- `total_available_balance`: Available balance
- `account_mm_rate`: Maintenance margin rate
- `total_maintenance_margin`: Total maintenance margin
- `created_at`: Record creation timestamp

### coin_greeks
- `id`: Primary key
- `timestamp`: API timestamp
- `base_coin`: Base coin symbol
- `total_delta`: Total delta value
- `total_gamma`: Total gamma value
- `total_vega`: Total vega value
- `total_theta`: Total theta value
- `created_at`: Record creation timestamp

## Usage

Run the application:
```bash
python bb_acc.py
```

The application will:
1. Initialize database tables if they don't exist
2. Start the scheduler
3. Collect data every 5 minutes
4. Log all operations to stdout

To stop the application, press `Ctrl+C`.

## Functions

- `get_wallet_balance(session)`: Fetches wallet balance and saves to database
- `get_coin_greeks(session, base_coin=None)`: Fetches coin greeks and saves to database
- `scheduled_data_collection()`: Scheduled task runner
- `init_database()`: Creates database tables
- `main()`: Application entry point

## Logging

All operations are logged to stdout with timestamps including:
- Database initialization
- Data collection start/completion
- Errors and exceptions
- Scheduler events