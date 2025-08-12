#!/bin/bash
#SBATCH --job-name=rsds_statistics
#SBATCH --output=rsds.o%J
#SBATCH --error=rsds.o%J
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=256G
#SBATCH --cpus-per-task=1
#SBATCH --time=08:00:00
#SBATCH --mail-user=philipp.heinrich@hereon.de
#SBATCH --mail-type=END,FAIL
#SBATCH --account=bb1203

srun python3 calc_statistics.py -v rsds
