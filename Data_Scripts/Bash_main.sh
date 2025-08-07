#!/bin/bash
#SBATCH --job-name=CF
#SBATCH --output=CF.o%J
#SBATCH --error=CF.o%J
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=256G
#SBATCH --cpus-per-task=10
#SBATCH --time=08:00:00
#SBATCH --mail-user=philipp.heinrich@hereon.de
#SBATCH --mail-type=END,FAIL
#SBATCH --account=bb1203

srun python3 main.py -c ${SLURM_CPUS_PER_TASK}