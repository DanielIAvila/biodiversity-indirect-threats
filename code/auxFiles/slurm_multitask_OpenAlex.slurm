#!/bin/bash
#SBATCH --job-name=OpenAlex
#SBATCH --output=logs1/openalex%year%.out
#SBATCH --error=logs1/openalex%year%.err
#SBATCH --array=0-28         # Adjust to match line count in tasks file - 1 
#SBATCH --time=20:00:00 
#SBATCH --cpus-per-task=16
#SBATCH --mem=200G 
#SBATCH --partition=generic
#SBATCH --mail-user=avilaortega@iiasa.ac.at
#SBATCH --mail-type=BEGIN,END,FAIL

# Load Python environment
module purge
module load Python/3.11.5-GCCcore-13.2.0

# Define task
TASK_FILE="path_to_years_file/years.txt"                      # OBS! Remember to change according to path
TASK=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" $TASK_FILE)

YEAR=$(echo $TASK | awk '{print $1}')

# Run script
python3 path_to_python_environment/downloadDataCluster.py $YEAR      # OBS! Remember to change according to path
