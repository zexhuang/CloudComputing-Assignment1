#!/bin/bash
#SBATCH -p physical
#SBATCH --output=1node1core_result.txt
#SBATCH --error=1node1core_error.txt
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:10:00
module load Python/3.4.3-goolf-2015a
mpirun -np 1 python3 assignment1.py