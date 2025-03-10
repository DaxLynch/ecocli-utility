import pandas as pd
import datetime

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
    df['datetime_utc'] = pd.to_datetime(df['datetime_utc'], errors='coerce')
    
    # Fill missing carbon intensity with 400
    df['carbon_intensity'] = df['carbon_intensity'].fillna(400)
    return df


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
    # Ask user if they want the current UTC time
    choice = input("Do you want to use the current UTC time? (y/N): ").strip().lower()

    if choice.lower() == 'y':
        start_dt = datetime.datetime.utcnow()
    else:
        try:
            # Prompt user for separate components
            month_str = input("Enter month (1-12): ").strip()
            day_str = input("Enter day (1-31): ").strip()
            hour_str = input("Enter hour (0-23): ").strip()

            month = int(month_str)
            day = int(day_str)
            hour = int(hour_str)

            # Build a datetime with a fixed year of 2025 (UTC, no minutes/seconds)
            start_dt = datetime.datetime(2024, month, day, hour)
        except ValueError:
            print("Invalid date/time input. Falling back to current UTC time.")
            start_dt = datetime.datetime.utcnow()

    print(f"Using start time (UTC): {start_dt}")
    return start_dt

def get_carbon_data_for_24h(df_carbon, start_dt):
    """
    Return carbon data from start_dt to start_dt + 24 hours.
    """
    end_dt = start_dt + datetime.timedelta(hours=24)
    mask = (df_carbon['datetime_utc'] >= start_dt) & (df_carbon['datetime_utc'] < end_dt)
    subset = df_carbon[mask].sort_values(by='datetime_utc')
    print(subset)
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


def cli_menu():
    """
    Very simple CLI using input() prompts:
    1) Ask user which mode
    2) Guide them accordingly.
    """
    print("Welcome to the CI/CD Carbon-Aware Scheduler!")
    print("[1] Workflow-Based Recommendation (Historical CI/CD Data)")
    print("[2] Custom Job-Based Recommendation")
    mode = input("Select a mode (1 or 2): ").strip()

    return mode


def main():
    # Load data
    df_cicd = load_cicd_data("cicdOps.csv")
    df_carbon = load_carbon_data("US_2024_hourly.csv")

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

        # 5) Retrieve carbon data for next 24 hours
        subset_carbon = get_carbon_data_for_24h(df_carbon, start_dt)
        if subset_carbon.empty:
            print("No carbon data for that date/time range!")
            return
        
        # 6) Perform sliding window
        carbon_series = subset_carbon['carbon_intensity'].reset_index(drop=True)
        best_index, best_sum = find_best_start_time(carbon_series, avg_run_hrs)
        if best_index is None:
            print("Could not find a valid window.")
            return

        # 7) The best start time is the date/time at subset_carbon.iloc[best_index]
        best_start_time = subset_carbon.iloc[best_index]['datetime_utc']
        print(f"\nBest Start Time: {best_start_time} UTC")
        print(f"Estimated carbon intensity sum over the window: {best_sum:.2f}")

    elif mode == "2":
        # --- Mode 2: Custom job-based ---
        # 1) Ask user for job duration
        duration_str = input("Enter job duration in hours (e.g. '2' for 2 hours): ").strip()
        try:
            job_duration = float(duration_str)
        except ValueError:
            job_duration = 1.0  # default 1 hour

        # 2) Ask user for location (not used, but we store it for future)
        location_str = input("Enter job location (currently unused): ").strip()
        if not location_str:
            location_str = "USA"

        # 3) Ask user for target date/time
        start_dt = get_user_start_datetime()

        # 4) Get carbon data for next 24 hours
        subset_carbon = get_carbon_data_for_24h(df_carbon, start_dt)
        if subset_carbon.empty:
            print("No carbon data for that date/time range!")
            return
        
        # 5) Perform sliding window
        carbon_series = subset_carbon['carbon_intensity'].reset_index(drop=True)
        best_index, best_sum = find_best_start_time(carbon_series, job_duration)
        if best_index is None:
            print("Could not find a valid window.")
            return

        best_start_time = subset_carbon.iloc[best_index]['datetime_utc']
        print(f"\nBest Start Time: {best_start_time} UTC")
        print(f"Estimated carbon intensity sum: {best_sum:.2f}")

    else:
        print("Invalid mode selected. Exiting.")


if __name__ == "__main__":
    main()

