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
import requests
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

def fetchHigreMetadataByY(y,name,token):
  url = 'https://agaveauth.its.hawaii.edu/meta/v2/data/'
  query = {'value.y':y,'value.name': name}
  payload = {'q': str(query),'limit':2000}
  headers = {"Authorization":"Bearer "+token,'content-type': 'application/json', 'Accept-Charset': 'UTF-8',}
  req = requests.get(url, params=payload, headers=headers, verify=False)
  print(req.text)
  return(json.loads(req.text)['result'])

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
  with open("landuse"+str(i)+"_"+str(j)+".json", 'w') as outfile:
      outfile.write(json.dumps(body, use_decimal=True))
  #print('[x: '+str(i)+',j: '+str(j) +'] '+ js['recharge_scenario0'])
  call("~/apps/cli/bin/metadata-addupdate -z "+ token +"  -F landuse"+str(i)+"_"+str(j)+".json;rm /tmp/landuse"+str(i)+"_"+str(j)+".json", shell=True)


def main(argv):
   global f
   x_offset = 0
   x_divisor = 1
   inputfile =""
   threads = 1
   name = "test-set"
   try:
      opts, args = getopt.getopt(argv,"hn:y:i:k",["name=","y_value=","ifile=","token="])
   except getopt.GetoptError:
      print('TRY ingest_nc_to_ike.py -n <dataset name> -y <y value> -i <inputfile> -k <token>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print ('ingest_nc_to_ike.py -n <dataset name> -y <y value>  -i <inputfile> -k <token>')
         sys.exit()
      elif opt in ("-n", "--name"):
         name= arg
      elif opt in ("-y", "--y_value"):
         y_value= int(arg)
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-k", "--token"):
         token = arg
      else:
         assert False, "unhandled option"
   print('Input file is "', inputfile)
   if os.path.isfile(inputfile) != True:
     print('Input file does not exist!')
     sys.exit()
   print('Input file confirmed')
   copyfile(inputfile, '/tmp/nc-file'+str(y_value)+'.nc')
   #read the netCDF file dimensions x,y,scenario
   f = nc.Dataset('/tmp/nc-file'+str(y_value)+'.nc','r+')
   x = np.array(f.variables['x'])
   y = np.array(f.variables['y'])
   print('X = ',x)
   print('Y= ',y)
   print('TOKEN: '+ token)
   scenario = np.array(f.variables['scenario'])
   c = list(range(0, 886))
   db_res = fetchHigreMetadataByY(y_value,name,token)
   for j in range(0,len(db_res)-1):
       c.remove(db_res[j]['value']['x'])
   print(c)
   for i in c:
     print('X index: ',str(i))
     createMetadata(y_value,i,x,y,name,token)

if __name__ == "__main__":
   main(sys.argv[1:])

