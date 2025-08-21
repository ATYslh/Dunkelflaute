#!/bin/bash
#SBATCH --job-name=CF_Wind
#SBATCH --output=CF.o%J
#SBATCH --error=CF.o%J
#SBATCH --partition=compute
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=32G
#SBATCH --cpus-per-task=36
#SBATCH --time=08:00:00
#SBATCH --mail-user=philipp.heinrich@hereon.de
#SBATCH --mail-type=END,FAIL
#SBATCH --account=bb1203

srun python3 CF_Wind_statistics.py -c ${SLURM_CPUS_PER_TASK}
