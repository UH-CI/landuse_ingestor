#!/usr/bin/python
import json
from functools import partial
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
from shutil import copyfile
from decimal import *
import simplejson as json
getcontext().prec = 3
#set projection convert from p1 to p2
#p1 = Proj(init='epsg:32604')
p1 = Proj(init='epsg:26904')
p2 = Proj(init='epsg:4326')
f=''
#read the netCDF file dimensions x,y,scenario
#f = nc.Dataset('input.nc','r+')
#x = np.array(f.variables['x'])
#y = np.array(f.variables['y'])
#scenario = np.array(f.variables['scenario'])

#set static json body values and permsissions
#body={}

#body['name'] = "Landuse"
#body['schemaId'] = "8102046857967243751-242ac1110-0001-013"
#body['permissions']=[pem1,pem2,pem4]

def createMetadata(j,i,x,y,dataset_name,token):
  print('create['+str(i)+','+str(j)+']')
  global f
  coord = transform(p1,p2,x[i],y[j])
  js ={}
  js['name'] = dataset_name
  js['longitude'] = coord[0]+0.00033687
  js['latitude'] = coord[1]+0.00033687
  js['x'] = i
  js['y'] = j
  #js['scenario'] = s
  js['loc'] = {"type":"Point", "coordinates":[js['longitude'],js['latitude']]}
  js['recharge_scenario0'] = list(map(lambda x: x if x is None else round(float(x),3),f.variables['recharge'][0,:,j,i].tolist())) #list(np.around(np.array(f.variables['recharge'][0,:,j,i].tolist()),2))
  js['recharge_scenario1'] = list(map(lambda x: x if x is None else round(float(x),3),f.variables['recharge'][1,:,j,i].tolist())) #list(np.around(np.array(f.variables['recharge'][1,:,j,i].tolist()),2))
  body={}
  body['name'] = "Landuse"
  body['schemaId'] = "8102046857967243751-242ac1110-0001-013"
  body['value'] = js
  body['geospatial']= True;
  with open("/tmp/landuse"+str(i)+"_"+str(j)+".json", 'w') as outfile:
      outfile.write(json.dumps(body, use_decimal=True))
  #print('[x: '+str(i)+',j: '+str(j) +'] '+ js['recharge_scenario0']) 
  call("~/apps/cli/bin/metadata-addupdate -z "+ token +"  -F /tmp/landuse"+str(i)+"_"+str(j)+".json;rm /tmp/landuse"+str(i)+"_"+str(j)+".json", shell=True)


def main(argv):
   global f
   x_offset = 0
   x_divisor = 1
   inputfile =""
   threads = 1
   name = "test-set"
   try:
      opts, args = getopt.getopt(argv,"hn:o:d:i:t:k",["name=","offset=","divisor=","ifile=","threads=","token="])
   except getopt.GetoptError:
      print('TRY ingest_nc_to_ike.py -n <dataset name> -o <x offset> -d <x divisor> -i <inputfile> -t <threads> -k <token>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print ('ingest_nc_to_ike.py -n <dataset name> -o <x offset> -d <x divisor> -i <inputfile> -t <threads> -k <token>')
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
      elif opt in ("-k", "--token"):
         token = arg
      else:
         assert False, "unhandled option"
   print('Input file is "', inputfile)
   if os.path.isfile(inputfile) != True:
     print('Input file does not exist!')
     sys.exit()
   print('Input file confirmed')
   copyfile(inputfile, '/tmp/nc-file'+str(x_offset)+'.nc')
   #read the netCDF file dimensions x,y,scenario
   f = nc.Dataset('/tmp/nc-file'+str(x_offset)+'.nc','r+')
   x = np.array(f.variables['x'])
   y = np.array(f.variables['y'])
   print('X = ',x)
   print('Y= ',y)
   print('TOKEN: '+ token)
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
       pool = multiprocessing.Pool(threads)
       for i in range(x_start,x_range):
       	 #result = [pool.apply(createMetadata, args=(j, i, x, y, name)) for j in range(0,len(y))]
         print('X index: ',str(i))
         pool = multiprocessing.Pool(threads)
         partialCreateMetadata = partial(createMetadata, i=i, x=x, y=y, dataset_name=name, token=token)
         pool.map(partialCreateMetadata,range(0,len(y))) 
         #loop through y (lat)
         #for j in range(0, len(y)):
         #  createMetadata(i,j,name)
	 #call("~/apps/cli/bin/auth-tokens-refresh",shell=True)
         #resutl = Parallel(n_jobs=threads)(delayed(createMetadata)(i,j,name) for j in range(0,len(y)))
         pool.close() #we are not adding any more processes
         pool.join() 
   else:
     print("x_divisor must be greater than 0")

if __name__ == "__main__":
   main(sys.argv[1:])
