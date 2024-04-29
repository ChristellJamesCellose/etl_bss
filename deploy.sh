#!/bin/bash

# Define the list of repository names
repo_names=('paytv' 'customer' 'otp' 'payment')

# Loop through each repository name
for repo_name in "${repo_names[@]}"; do
    echo "Deploying for repository: $repo_name"
    
    # Build command
    build_command="prefect deployment build ./etl_bss_${repo_name}/main.py:etl_data_${repo_name} -n ETL_${repo_name} -sb github/etl-bss"

    # Apply command
    apply_command="prefect deployment apply etl_data_${repo_name}-deployment.yaml"

    # Execute the commands
    echo "Building ETL for ${repo_name}..."
    echo "Running: $build_command"
    $build_command

    echo "Applying deployment for ${repo_name}..."
    echo "Running: $apply_command"
    $apply_command

    echo "Deployment for repository $repo_name completed."
done

echo "Process completed."

