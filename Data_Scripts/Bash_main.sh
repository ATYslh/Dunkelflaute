#!/bin/bash
#SBATCH --job-name=CF
#SBATCH --output=CF.o%J
#SBATCH --error=CF.o%J
#SBATCH --partition=shared
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=128G
#SBATCH --cpus-per-task=5
#SBATCH --time=72:00:00
#SBATCH --mail-user=philipp.heinrich@hereon.de
#SBATCH --mail-type=END,FAIL
#SBATCH --account=bb1203

srun python3 main.py -c ${SLURM_CPUS_PER_TASK}