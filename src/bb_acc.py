from pybit.unified_trading import HTTP
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from datetime import datetime

# Configure logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Load API credentials
load_dotenv('.env')
api_key = os.getenv('API_BB_OPTION')
api_secret = os.getenv('API_BB_OPTION_KEY')

# Database connection
def get_db_connection():
    """Get database connection using psycopg2"""
    return psycopg2.connect(
        host=os.getenv('NEXUS_PG_HOST'),
        port=os.getenv('NEXUS_PG_PORT'),
        user=os.getenv('NEXUS_PG_USER'),
        password=os.getenv('NEXUS_PG_PASSWORD'),
        database=os.getenv('NEXUS_PG_DATABASE')
    )

def init_database():
    """Initialize database tables"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create wallet_balance table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_balance (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            total_equity DECIMAL(20, 8),
            account_im_rate DECIMAL(10, 8),
            total_margin_balance DECIMAL(20, 8),
            total_initial_margin DECIMAL(20, 8),
            total_available_balance DECIMAL(20, 8),
            account_mm_rate DECIMAL(10, 8),
            total_maintenance_margin DECIMAL(20, 8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create coin_greeks table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS coin_greeks (
            id SERIAL PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            base_coin VARCHAR(10),
            total_delta DECIMAL(20, 8),
            total_gamma DECIMAL(20, 8),
            total_vega DECIMAL(20, 8),
            total_theta DECIMAL(20, 8),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Database tables initialized")

def get_wallet_balance(session: HTTP):
    """
    Get wallet balance information.
    """
    res = session.get_wallet_balance(
        accountType="UNIFIED",
        coin="USDT"
    )
    
    if res['retCode'] != 0:
        logging.error(f"Failed to get wallet balance: {res['retMsg']}")
        return None
        
    account_info = res['result']['list'][0]
    
    ts = res['time']
    totalEquity = float(account_info['totalEquity'])
    accountIMRate = float(account_info['accountIMRate'])
    totalMarginBalance = float(account_info['totalMarginBalance'])
    totalInitialMargin = float(account_info['totalInitialMargin'])
    totalAvailableBalance = float(account_info['totalAvailableBalance'])
    accountMMRate = float(account_info['accountMMRate'])
    totalMaintenanceMargin = float(account_info['totalMaintenanceMargin'])

    wallet_data = {
        "timestamp": ts,
        "totalEquity": totalEquity,
        "accountIMRate": accountIMRate,
        "totalMarginBalance": totalMarginBalance,
        "totalInitialMargin": totalInitialMargin,
        "totalAvailableBalance": totalAvailableBalance,
        "accountMMRate": accountMMRate,
        "totalMaintenanceMargin": totalMaintenanceMargin
    }
    
    # Save to database
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO wallet_balance (timestamp, total_equity, account_im_rate, 
                                  total_margin_balance, total_initial_margin, 
                                  total_available_balance, account_mm_rate, 
                                  total_maintenance_margin)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (ts, totalEquity, accountIMRate, totalMarginBalance, 
          totalInitialMargin, totalAvailableBalance, accountMMRate, 
          totalMaintenanceMargin))
    conn.commit()
    cur.close()
    conn.close()
    
    logging.info(f"Retrieved and saved wallet balance - Total Equity: {totalEquity}")
    return wallet_data

def get_coin_greeks(session: HTTP, base_coin: str | None = None):
    """
    Get coin greeks information.
    """
    params = {}
    if base_coin:
        params['baseCoin'] = base_coin
        
    res = session.get_coin_greeks(**params)
    
    if res['retCode'] != 0:
        logging.error(f"Failed to get coin greeks: {res['retMsg']}")
        return []
    
    result = res['result']
    
    greeks_info = result.get("list", [{}])
    ts = res['time']

    greeks_data = [item | {"timestamp": ts} for item in greeks_info]
    
    # Save to database
    if greeks_data:
        conn = get_db_connection()
        cur = conn.cursor()
        for item in greeks_data:
            cur.execute("""
                INSERT INTO coin_greeks (timestamp, base_coin, total_delta, 
                                       total_gamma, total_vega, total_theta)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (item.get('timestamp'), item.get('baseCoin'), 
                  item.get('totalDelta'), item.get('totalGamma'), 
                  item.get('totalVega'), item.get('totalTheta')))
        conn.commit()
        cur.close()
        conn.close()
    
    logging.info(f"Retrieved and saved {len(greeks_data)} coin greeks records")
    return greeks_data

def scheduled_data_collection():
    """Scheduled task to collect wallet balance and coin greeks data"""
    try:
        session = HTTP(
            testnet=False,
            api_key=api_key,
            api_secret=api_secret,
        )
        
        logging.info("Starting scheduled data collection...")
        
        # Collect wallet balance
        get_wallet_balance(session)
        
        # Collect coin greeks
        get_coin_greeks(session)
        
        logging.info("Scheduled data collection completed successfully")
        
    except Exception as e:
        logging.error(f"Error in scheduled data collection: {e}")

def run_scheduler():
    """Main function to run the scheduler"""
    # Initialize database
    init_database()
    
    # Set up scheduler
    scheduler = BlockingScheduler()
    scheduled_data_collection()
    # Schedule the job to run every 5 minutes
    scheduler.add_job(
        scheduled_data_collection,
        'interval',
        minutes=5,
        id='data_collection_job'
    )
    
    logging.info("Starting scheduler - data collection will run every 5 minutes")
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logging.info("Scheduler stopped by user")
        scheduler.shutdown()
