import os
import copy  # Ensures deep copying of steps
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import SingleQuotedScalarString

# Initialize YAML parser (preserves formatting)
yaml = YAML()
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=4, offset=2)

# EcoCI steps
start_ecoci_step = {
    "name": "Start Energy Measurement",
    "uses": "DaxLynch/eco-ci-energy-estimation@main",
    "with": {
        "task": "start-measurement",
        "api-endpoint-add": "${{ secrets.ECOCI_API }}"  # Keep secret API token
    }
}

show_ecoci_step = {
    "name": "Show Energy Results",
    "uses": "DaxLynch/eco-ci-energy-estimation@main",
    "with": {
        "task": "display-results"
    }
}

def add_ecoci_to_workflow(file_path):
    """Reads a workflow file, inserts EcoCI steps if missing, and saves the file."""
    with open(file_path, "r") as f:
        data = yaml.load(f)

    if "jobs" not in data:
        print(f"⚠️ Skipping {file_path} (No 'jobs' key found)")
        return
    
    updated = False

    # Iterate through all jobs in the workflow
    for job_name, job_data in data["jobs"].items():
        if "steps" not in job_data:
            continue  # Skip jobs with no steps

        steps = job_data["steps"]
        
        # Check if EcoCI is already integrated
        if any(step.get("uses", "").startswith("DaxLynch/eco-ci-energy-estimation") for step in steps):
            print(f"✅ EcoCI already present in {file_path} -> {job_name}")
            continue

        # Insert "Start Energy Measurement" at the **beginning**
        steps.insert(0, copy.deepcopy(start_ecoci_step))

        # Insert "Get Measurement" before the last step (before "Show Energy Results")
        get_measurement_step = {
            "name": f"{job_name} Energy Measurement",
            "uses": "DaxLynch/eco-ci-energy-estimation@main",
            "with": {
                "task": "get-measurement",
                "label": SingleQuotedScalarString(job_name)  # Use job name dynamically
            }
        }

        # Ensure "Show Energy Results" is always **the last step**
        if any(step.get("uses", "") == "DaxLynch/eco-ci-energy-estimation@main" and step.get("with", {}).get("task") == "display-results" for step in steps):
            # Move "Show Energy Results" to the last position if it exists
            steps[:] = [s for s in steps if s != show_ecoci_step] + [copy.deepcopy(show_ecoci_step)]
        else:
            # Append "Get Measurement" and "Show Energy Results"
            steps.append(copy.deepcopy(get_measurement_step))
            steps.append(copy.deepcopy(show_ecoci_step))

        updated = True
        print(f"🔄 Added EcoCI to {file_path} -> {job_name}")

    if updated:
        # Write the modified YAML back to the file **without anchors**
        with open(file_path, "w") as f:
            yaml.dump(data, f)

# Find all YAML files in `.github/workflows/`
workflow_dir = ".github/workflows"
for filename in os.listdir(workflow_dir):
    if filename.endswith(".yml") or filename.endswith(".yaml"):
        file_path = os.path.join(workflow_dir, filename)
        add_ecoci_to_workflow(file_path)

print("✅ EcoCI integration complete!")
