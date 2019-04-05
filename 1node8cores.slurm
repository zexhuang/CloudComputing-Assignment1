#!/bin/bash
#SBATCH -p cloud
#SBATCH --output=1node8cores_result.txt
#SBATCH --error=1node8cores_error.txt
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --time=00:10:00
module load Python/3.6.4-intel-2017.u2
mpirun -np 8 python3 assignment1.py