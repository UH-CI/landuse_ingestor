#!/usr/bin/python
import json
import subprocess
from subprocess import call
import sys, getopt
import os.path
import fileinput
import netCDF4 as nc
import numpy as np
from pyproj import Proj, transform
from joblib import Parallel, delayed
import multiprocessing


#set projection convert from p1 to p2
p1 = Proj(init='epsg:32604')
p2 = Proj(init='epsg:4326')

#read the netCDF file dimensions x,y,scenario
#f = nc.Dataset('input.nc','r+')
#x = np.array(f.variables['x'])
#y = np.array(f.variables['y'])
#scenario = np.array(f.variables['scenario'])

#set static json body values and permsissions
body={}
pem1={}
pem1['username']= 'seanbc'
pem1['permission']='ALL'
pem2={}
pem2['username']= 'jgeis'
pem2['permission']='ALL'
pem4={}
pem4['username']= 'ikewai-admin'
pem4['permission']='ALL'
pem1['username']= 'public'
pem1['permission']='READ'

body['name'] = "Landuse"
body['schemaId'] = "8102046857967243751-242ac1110-0001-013"
body['permissions']=[pem1,pem2,pem4]

def createMetadata(i,j,f,x,y,dataset_name):
  coord = transform(p1,p2,x[i],y[j])
  js ={}
  js['name'] = dataset_name
  js['longitude'] = coord[0]
  js['latitude'] = coord[1]
  js['x'] = i
  js['y'] = j
  #js['scenario'] = s
  js['loc'] = {"type":"Point", "coordinates":[js['longitude'],js['latitude']]}
  js['recharge_scenario0'] = f.variables['recharge'][0,:,j,i].tolist()
  js['recharge_scenario1'] = f.variables['recharge'][1,:,j,i].tolist()
  body['value'] = js
  body['geospatial']= True;
  with open("/tmp/landuse"+str(j)+".json", 'w') as outfile:
      json.dump(body, outfile)
  #print('x: '+str(i)+',j: '+str(j))
  #print(js['recharge_scenario0']) 
  call("~/apps/cli/bin/metadata-addupdate -F /tmp/landuse"+str(j)+".json;rm /tmp/landuse"+str(j)+".json", shell=True)


def main(argv):
   x_offset = 0
   x_divisor = 1
   inputfile =""
   threads = 1
   name = "test-set"
   try:
      opts, args = getopt.getopt(argv,"hn:o:d:i:t",["name=","offset=","divisor=","ifile=","threads="])
   except getopt.GetoptError:
      print('TRY ingest_nc_to_ike.py -n <dataset name> -o <x offset> -d <x divisor> -i <inputfile> -t <threads>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print ('ingest_nc_to_ike.py -n <dataset name> -o <x offset> -d <x divisor> -i <inputfile> -t <threads>')
         sys.exit()
      elif opt in ("-n", "--name"):
         name= arg
      elif opt in ("-o", "--offset"):
         x_offset= int(arg)
      elif opt in ("-d", "--divisor"):
         x_divisor= int(arg)
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-t", "--threads"):
         threads = int(arg)
      else:
         assert False, "unhandled option"
   print('Input file is "', inputfile)
   if os.path.isfile(inputfile) != True:
     print('Input file does not exist!')
     sys.exit()
   #read the netCDF file dimensions x,y,scenario
   f = nc.Dataset(inputfile,'r+')
   x = np.array(f.variables['x'])
   y = np.array(f.variables['y'])
   scenario = np.array(f.variables['scenario'])
   if x_divisor > 0:
     if x_divisor < len(x):
       x_step = int(len(x)/x_divisor)
       x_range = x_step * (x_offset + 1);
       x_start = x_offset * x_step
       if x_range > len(x):
         x_range = len(x)
       #loop through x (long)
       print(str(x_start)+'-'+str(x_range))
       for i in range(x_start,x_range):
         print(str(i))
         #loop through y (lat)
         #for j in range(0, len(y)):
         call("~/apps/cli/bin/auth-tokens-refresh",shell=True)
         Parallel(n_jobs=threads)(delayed(createMetadata)(i,j,f,x,y) for j in range(0,len(y)))
   else:
      print("x_divisor must be greater than 0")

if __name__ == "__main__":
   main(sys.argv[1:])
