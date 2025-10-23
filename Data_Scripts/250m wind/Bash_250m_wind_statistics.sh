#!/bin/bash
#SBATCH --job-name=250m
#SBATCH --output=250m.o%J
#SBATCH --error=250m.o%J
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=40G
#SBATCH --cpus-per-task=1
#SBATCH --time=04:00:00
#SBATCH --mail-user=philipp.heinrich@hereon.de
#SBATCH --mail-type=END,FAIL
#SBATCH --account=bb1203

srun python3 calc_250m_wind_statistics.py
