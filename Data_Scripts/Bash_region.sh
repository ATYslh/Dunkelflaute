#!/bin/bash
#SBATCH --job-name=Regional
#SBATCH --output=regional.o%J
#SBATCH --error=regional.o%J
#SBATCH --partition=shared
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=128G
#SBATCH --cpus-per-task=5
#SBATCH --time=72:00:00
#SBATCH --mail-user=philipp.heinrich@hereon.de
#SBATCH --mail-type=END,FAIL
#SBATCH --account=bb1203

srun python3 regional_data.py -c ${SLURM_CPUS_PER_TASK}