import schedule
import time
import subprocess
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    filename='scraper_scheduler.log'
)

def run_scraper():
    try:
        logging.info(f"Starting scraper at {datetime.now()}")
        result = subprocess.run(['python', 'main.py'], capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info("Scraper completed successfully")
            logging.info(result.stdout)
        else:
            logging.error("Scraper failed")
            logging.error(result.stderr)
    except Exception as e:
        logging.error(f"Error running scraper: {e}")

# Schedule the job to run every 1 hour
schedule.every(1).hour.do(run_scraper)

# Initial run
run_scraper()

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)
