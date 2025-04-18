import concurrent.futures
import subprocess
import os
import time

# Directory where your scripts are located
SCRIPTS_DIR = 'path/to/your/scripts'

# List of scripts to run
scripts = [
    '24h Volatility & Trading Range.py',
    'CoinDeskSentiment.py',
    'ConsumerConfidence.py',
    'CurrencyExchange.py',
    'GDPData.py',
    'GlobalGDP.py',
    'GoogleTrending.py',
    'InflationData.py',
    'InterestRate.py',
    'MarketCap.py',
    'PricesScript.py',
    'UnemploymentData.py',
    'VolumeTraded.py',
    'retailConsumption.py'
]

# Function to run a script
def run_script(script_name):
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    try:
        print(f"Running script: {script_name}")
        subprocess.run(['python', script_path], check=True)
        print(f"Script {script_name} completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error in script {script_name}: {e}")
    time.sleep(2)  # Small delay to avoid rate limiting or too many requests at once

# Main function to run scripts concurrently or sequentially
def run_all_scripts():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit all scripts for parallel execution
        futures = [executor.submit(run_script, script) for script in scripts]

        # Wait for all scripts to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()  # This will raise an exception if the script failed

if __name__ == '__main__':
    run_all_scripts()
