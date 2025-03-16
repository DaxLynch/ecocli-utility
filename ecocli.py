import pandas as pd
import datetime
import os
import re
import requests
from zoneinfo import ZoneInfo
import json
import psycopg2

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)  # Ensure directory exists


def load_cicd_data(cicd_csv_path):
    """
    Load the CI/CD data from CSV, adding the column headers manually.
    """
    columns = [
        "energy_uj", "cpu", "commit_hash", "repo", "branch", "workflow",
        "run_id", "label", "source", "cpu_util_avg", "duration_us", "workflow_name",
        "filter_type", "filter_project", "filter_machine", "filter_tags",
        "lat", "lon", "city", "ip", "carbon_intensity_g", "carbon_ug", "start_time",
        "min_carbon_intensity_g", "min_carbon_intensity_time"
    ]
    df = pd.read_csv(cicd_csv_path, header=None, names=columns)
    # Convert start_time to datetime if needed
    if 'start_time' in df.columns:
        df['start_time'] = pd.to_datetime(df['start_time'], errors='coerce')
    return df


def load_carbon_data(carbon_csv_path):
    """
    Load the US 2024 hourly carbon data.
    Datetime(UTC) is the first column. I'll parse that to datetime.
    """
    df = pd.read_csv(carbon_csv_path)
    df.rename(columns={
        "Datetime (UTC)": "datetime_utc",
        "Carbon Intensity gCO₂eq/kWh (direct)": "carbon_intensity"
    }, inplace=True)
    df['datetime_utc'] = pd.to_datetime(df['datetime_utc'], errors='coerce', utc="True")
    
    # Fill missing carbon intensity with 400. This is a temporary value, should be filled with the average of the data set, or some other interpolation method
    df['carbon_intensity'] = df['carbon_intensity'].fillna(400)
    return df

def get_zone_csv_for_location(lat, lon):
    """
    1) If the location is fedault, use the local csv,
    2) Otherwise find which zone the lat/lon is in by pinging the api
    3) extracting the zone key from the error response
    4) Check if CSV file already exists, and if so return that
    5) Load data into pandas
    """
    # Step 1: Check if location is default, in that case its already local
    if lat == 0.0 and lon == 0.0:
        print("Default US carbon data used")
        csv_filename = os.path.join(DATA_DIR, "US_2024_hourly.csv")
        return load_carbon_data(csv_filename)


    # Step 2: Otherwise fetch zone key from Electricity Maps API
    print(f"Fetching zone data for lat: {lat}, lon: {lon}...")

    url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?lat={lat}&lon={lon}"
    headers = {"auth-token": "DwDd0FXCLfD8W57FToNf"}  # This is my personal token

    try:
        response = requests.get(url, headers=headers)
        response_json = response.json()
    except Exception as e:
        print("Error during HTTP request:", e)
        return None

    # Step 3: Extract `zoneKey` from API response error message
    err_msg = response_json.get("error", "")
    match = re.search(r'zoneKey=([^,]+)', err_msg)
    if not match:
        print("Could not determine zoneKey from response:", response_json)
        return None
    zone_key = match.group(1)
    print(f"Detected zone key: {zone_key}")
    csv_filename = None

    csv_filename = os.path.join(DATA_DIR, f"{zone_key}_2024_hourly.csv")

    # Step 4: Check if CSV file already exists, and if so return that
    if os.path.exists(csv_filename):
        print(f"📂 Using cached data from {csv_filename}")
        return load_carbon_data(csv_filename)

    # Step 4: Construct CSV download URL
    csv_url = f"https://data.electricitymaps.com/2025-01-27/{zone_key}_2024_hourly.csv"

    try:
        dl_response = requests.get(csv_url)
        if dl_response.status_code != 200:
            print(f"Failed to download CSV for zone {zone_key} (status {dl_response.status_code}).")
            return None

        with open(csv_filename, "wb") as f:
            f.write(dl_response.content)
        print(f"Saved {csv_filename} for future use.")

    except Exception as e:
        print("Error downloading CSV:", e)
        return None

    # Step 5: Load data into pandas
    return load_carbon_data(csv_filename)


def prompt_for_location_and_download():
    """
    Asks the user for latitude & longitude and attempts to download the
    relevant zone's CSV from electricitymaps.com, returning a DataFrame.
    """
    # Example lat/lon prompt
    lat_str = input("Enter latitude (e.g., 45): ").strip()
    lon_str = input("Enter longitude (e.g., -123): ").strip()

    # Convert to float
    try:
        lat = float(lat_str)
        lon = float(lon_str)
    except ValueError:
        print("Invalid lat/lon, defaulting to some fallback (e.g., 0,0).")
        lat, lon = 0.0, 0.0

    df_zone = get_zone_csv_for_location(lat, lon)
    if df_zone is None or df_zone.empty:
        print("Could not retrieve zone CSV or data is empty.")
        return None
    print("Zone CSV loaded successfully!")
    return df_zone

def get_unique_repos(df_cicd):
    """Return a sorted list of unique repos."""
    return sorted(df_cicd['repo'].dropna().unique())


def get_workflows_for_repo(df_cicd, repo_name):
    """Return a sorted list of workflows for a given repo."""
    subset = df_cicd[df_cicd['repo'] == repo_name]
    return sorted(subset['workflow_name'].dropna().unique())


def get_average_runtime_hours(df_cicd, repo_name, workflow_name):
    """
    Get the average runtime for the selected repo & workflow.
    'duration_us' is in microseconds. Convert to hours.
    """
    subset = df_cicd[(df_cicd['repo'] == repo_name) &
                     (df_cicd['workflow'] == workflow_name)]
    if subset.empty:
        return 1.0  # default to 1 hour if we have no data
    avg_duration_us = subset['duration_us'].mean()
    avg_hours = avg_duration_us / (1e6 * 3600)  # microseconds -> hours
    return max(avg_hours, 0.5)  # enforce a minimum of 0.5 hr

def get_user_start_datetime():
    # Ask user if they want the current time
    choice = input("Do you want to use the current time? (y/N): ").strip().lower()

    if choice.lower() == 'y':
        start_dt = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=365)
    else:
        try:
            # Prompt user for separate components
            month_str = input("Enter month (1-12): ").strip()
            day_str = input("Enter day (1-31): ").strip()
            hour_str = input("Enter hour (0-23): ").strip()

            month = int(month_str)
            day = int(day_str)
            hour = int(hour_str)

            pnw_zone = ZoneInfo("America/Los_Angeles") #Assuming tz is pnw. Maybe we should get local
            naive_dt = datetime.datetime(2024, month, day, hour)
            # Assign the PNW timezone:
            start_dt = naive_dt.replace(tzinfo=pnw_zone)
        except ValueError:
            print("Invalid date/time input. Falling back to current UTC time.")
            start_dt = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=365)

    print(f"Using start time (UTC): {start_dt}")
    return start_dt

def get_carbon_data_for_24h(df_carbon, start_dt):
    """
    Return carbon data from start_dt to start_dt + 24 hours.
    """
    end_dt = start_dt + datetime.timedelta(hours=24)
    mask = (df_carbon['datetime_utc'] >= start_dt) & (df_carbon['datetime_utc'] < end_dt)
    subset = df_carbon[mask].sort_values(by='datetime_utc')
    #print(subset)
    return subset

def find_best_start_time(carbon_series, duration_hours):
    """
    Given a time-ordered Series of carbon intensities (one value per hour),
    slide over each possible starting hour, summing over 'duration_hours' hours.
    
    NOTE: For simplicity, we assume 'duration_hours' can be rounded up or down
    to an integer. If you need partial-hour logic, you'll need a more detailed approach.
    """
    # Round duration hours to nearest integer for discrete hourly sums
    dur_hrs = int(round(duration_hours))
    if dur_hrs <= 0:
        dur_hrs = 1

    best_sum = float('inf')
    best_start_index = None

    # We can only slide up to len(carbon_series) - dur_hrs + 1
    for start_idx in range(0, len(carbon_series) - dur_hrs + 1):
        window_sum = carbon_series.iloc[start_idx:start_idx + dur_hrs].sum()
        if window_sum < best_sum:
            best_sum = window_sum
            best_start_index = start_idx

    if best_start_index is not None:
        return best_start_index, best_sum
    else:
        return None, None
    
def find_worst_start_time(carbon_series, duration_hours):
    """
    Given a time-ordered Series of carbon intensities (one value per hour),
    slide over each possible starting hour, summing over 'duration_hours' hours.
    
    NOTE: For simplicity, we assume 'duration_hours' can be rounded up or down
    to an integer. If you need partial-hour logic, you'll need a more detailed approach.
    """
    # Round duration hours to nearest integer for discrete hourly sums
    dur_hrs = int(round(duration_hours))
    if dur_hrs <= 0:
        dur_hrs = 1

    worst_sum = -float('inf')
    worst_start_index = None

    # We can only slide up to len(carbon_series) - dur_hrs + 1
    for start_idx in range(0, len(carbon_series) - dur_hrs + 1):
        window_sum = carbon_series.iloc[start_idx:start_idx + dur_hrs].sum()
        if window_sum > worst_sum:
            worst_sum = window_sum
            worst_start_index = start_idx

    if worst_start_index is not None:
        return worst_start_index, worst_sum
    else:
        return None, None


def cli_menu():
    """
    Very simple CLI using input() prompts:
    1) Ask user which mode
    2) Guide them accordingly.
    """
    print("Welcome to the CI/CD Carbon-Aware Scheduler!")
    print("[1] Workflow-Based Recommendation (Historical CI/CD Data)")
    print("[2] Custom Job-Based Recommendation")
    print("[3] Sync local SQL Database")
    mode = input("Select a mode (1, 2 or 3): ").strip()

    return mode


def main():
    # Load data
    df_cicd = load_cicd_data("data/cicdOps.csv")

    # Prompt user for mode
    mode = cli_menu()

    if mode == "1":
        # --- Mode 1: Workflow-based ---
        # 1) List repos
        repos = get_unique_repos(df_cicd)
        if not repos:
            print("No repositories found in CI/CD data!")
            return
        print("\nAvailable Repositories:")
        for i, r in enumerate(repos, start=1):
            print(f"{i}. {r}")

        repo_choice = input("Choose a repository by number: ").strip()
        try:
            repo_idx = int(repo_choice) - 1
            chosen_repo = repos[repo_idx]
        except (ValueError, IndexError):
            print("Invalid choice. Exiting.")
            return

        # 2) List workflows
        workflows = get_workflows_for_repo(df_cicd, chosen_repo)
        if not workflows:
            print(f"No workflows found for repo '{chosen_repo}'. Exiting.")
            return
        print(f"\nWorkflows in {chosen_repo}:")
        for i, w in enumerate(workflows, start=1):
            print(f"{i}. {w}")

        wf_choice = input("Choose a workflow by number: ").strip()
        try:
            wf_idx = int(wf_choice) - 1
            chosen_workflow = workflows[wf_idx]
        except (ValueError, IndexError):
            print("Invalid choice. Exiting.")
            return

        # 3) Compute average runtime
        avg_run_hrs = get_average_runtime_hours(df_cicd, chosen_repo, chosen_workflow)
        print(f"Average runtime for {chosen_workflow} is ~{avg_run_hrs:.2f} hours.")

        # 4) Ask user for target date (UTC). Default: today
        start_dt = get_user_start_datetime()

        # 5) Ask user for location they want to run the operation
        df_carbon = prompt_for_location_and_download()

        # 6) Retrieve carbon data for next 24 hours
        subset_carbon = get_carbon_data_for_24h(df_carbon, start_dt)
        if subset_carbon.empty:
            print("No carbon data for that date/time range!")
            return
        
        # 7) Perform sliding window
        carbon_series = subset_carbon['carbon_intensity'].reset_index(drop=True)
        best_index, best_sum = find_best_start_time(carbon_series, avg_run_hrs)
        if best_index is None:
            print("Could not find a valid window.")
            return

        # 7) The best start time is the date/time at subset_carbon.iloc[best_index]
        best_start_time = subset_carbon.iloc[best_index]['datetime_utc']
        print(f"\nBest Start Time: {best_start_time} UTC")
        print(f"Estimated carbon intensity sum over the window: {best_sum:.2f}")
        
        # 7) Get worst time
        worst_index, worst_sum = find_worst_start_time(carbon_series, avg_run_hrs)
        if worst_index is None:
            print("Could not find a valid window.")
            return

        # 7) The worst start time is the date/time at subset_carbon.iloc[best_index]
        worst_start_time = subset_carbon.iloc[worst_index]['datetime_utc']
        print(f"\nWorst Start Time: {worst_start_time} UTC")
        print(f"Estimated carbon intensity sum: {worst_sum:.2f}")

    elif mode == "2":
        # --- Mode 2: Custom job-based ---
        # 1) Ask user for job duration
        duration_str = input("Enter job duration in hours (e.g. '2' for 2 hours): ").strip()
        try:
            job_duration = float(duration_str)
        except ValueError:
            job_duration = 1.0  # default 1 hour

        # 2) Ask user for location (not used, but we store it for future)
        df_carbon = prompt_for_location_and_download()

       # 3) Ask user for target date/time
        start_dt = get_user_start_datetime()

        #Get the correct df_carbon based on the location

        # 4) Get carbon data for next 24 hours
        subset_carbon = get_carbon_data_for_24h(df_carbon, start_dt)
        if subset_carbon.empty:
            print("No carbon data for that date/time range!")
            return
        
        # 5) Perform sliding window
        carbon_series = subset_carbon['carbon_intensity'].reset_index(drop=True)
        # Get best time
        best_index, best_sum = find_best_start_time(carbon_series, job_duration)
        if best_index is None:
            print("Could not find a valid window.")
            return

        best_start_time = subset_carbon.iloc[best_index]['datetime_utc']
        print(f"\nBest Start Time: {best_start_time} UTC")
        print(f"Estimated carbon intensity sum: {best_sum:.2f}")
        # Get worst time
        worst_index, worst_sum = find_worst_start_time(carbon_series, job_duration)
        if worst_index is None:
            print("Could not find a valid window.")
            return

        worst_start_time = subset_carbon.iloc[worst_index]['datetime_utc']
        print(f"\nWorst Start Time: {worst_start_time} UTC")
        print(f"Estimated carbon intensity sum: {worst_sum:.2f}")

    elif mode == "3":
        dbname = input("What is the database name? ").strip()
        user = input("What is the username? ").strip()
        password = input("What is the password? ")
        host = input("What is the host IP? ").strip()
        port = input("What is the port? ").strip()
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        df = pd.read_sql("SELECT * FROM api.environmental_data", conn)
        df.to_csv("data/cicdOps.csv", index=False)

    else:
        print("Invalid mode selected. Exiting.")


if __name__ == "__main__":
    main()

