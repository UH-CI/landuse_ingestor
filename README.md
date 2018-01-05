Ingestion script for reading landuse NetCDF data and serializing to JSON and storing in the IKE Gateway.

This script is capable of serializing netcdf data in the correct format to an Agave MongoDB store as metadata JSON objects. This script can be run in jobarrays as flags for offset provide a way to break up the matrix for parallel ingestion.

Note that you will need to install all the dependencies like netcdf etc that are needed -see the top of the file for all the dependencies.  I recommend you install NetCDF with conda and use a conda virtualenv.


To run for testing use:
```
python3 ingest_nc_to_ike.py -n testunit3 -o 0 -d 1 -i input.nc --threads=20
```
-n, --name : is the name to give to this dataset to distinguish from other serialized data - it will be stored as 'value.name' in the JSON metadata object
-o, --offset : this is the ofsset to use is running in high throughput parallelization.  0 is the starting offset - the offset will increase the starting matrix index so the full matrix is covered by all child array processes
-d, --divsor : this is the number of total jobarray jobs and this tells the script how to chop up the matrix for no-overlap processesing
-i, --inputfile : this is the path to the inputfile - it will be copied to /tmp on each node for processing 
-t, --threads : this is the number of python threads to use on each node

Below is an example Slurm job array script that runs a 5 job array - lets call it ingest_array.slurm:
```
#!/bin/bash
#SBATCH -J ingest_landuse # A single job name for the array
#SBATCH -c 20 # Number of cores
#SBATCH -N 1 # All tasks on one machine
#SBATCH -p community.q# Partition
#SBATCH -t 2880# 2 hours (D-HH:MM)
#SBATCH -o ingest_landuse%A-%a.out # Standard output# %A" is replaced by the job ID and "%a" with the array index
#SBATCH -e ingest_landuse%A-%a.err # Standard error
source ~/.bash_profile
source ~/miniconda3/envs/ike-ingest/bin/activate ike-ingest
python3 ingest_nc_to_ike.py -n testset -o $SLURM_ARRAY_TASK_ID -d 5 -i input.nc --threads=20
```

To launch the job array:
```
sbatch --array=0-4 ingest_array.slurm 
```

