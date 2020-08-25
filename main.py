import os, fnmatch
from datetime import datetime, timedelta, time, date

##Local Functions in fnc
from fnc.GeneralFunctions import process_agent_file, process_productivity_file, incident_details, process_not_ready_file, process_SD_file
spath = ''
dpath = ''
os.system('del {}\\excel\\SQ-MPC*.*'.format(dpath))
file_count = os.listdir("{}".format(spath))
pattern = 'SQ-MPC*{}*'.format(date.today().strftime("%Y-%m-%d"))
for element in file_count:
    if fnmatch.fnmatch(element,pattern):
         os.system('cp "{}{}" {}\\excel'.format(spath,element,dpath))

for file in os.listdir('./excel'):
    print(file)
    if 'mpc006' in file.lower(): #Agent
        process_agent_file(file)
    elif 'mpc003' in file.lower():
        process_productivity_file(file)
    elif 'mpc008' in file.lower():
        incident_details(file)
    elif 'mpc007' in file.lower():
        process_not_ready_file(file)
    elif 'mpc004' in file.lower():
        process_SD_file(file)
