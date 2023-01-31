##Clean code to download 1 variable
import os
import wget
import time
from datetime import date
from datetime import timedelta
from datetime import datetime

#download 24 hours of previous day for a specific variable
# iterate over 24/3

#destination_folder="C:/Users/baatz/DWD210/downloads"
#destination_folder = "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/dwd_spreewasserN/forecast_daily/raw2/"
destination_folder="/beegfs/common/data/climate/dwd_spreewasserN/forecast_daily/raw/"
weather_variables=["alb_rad","t_2m","tmax_2m","tmin_2m","qv_s","relhum_2m","v_10m","u_10m","pmsl","w_snow"]

#dwd_path="https://opendata.dwd.de/weather/nwp/icon/grib/"
file_ending=".grib2.bz2"

dwd_path="https://opendata.dwd.de/weather/nwp/icon-d2-eps/grib/"
simulation_name="icon-d2-eps_germany_icosahedral_single-level_"

today = date.today()
end_day= today
start_day= today - timedelta(days = 1)

for weather_variable in weather_variables:
    field_name="2d_"+weather_variable
    cycle_time=[start_day,end_day]
    for the_day in cycle_time:
        for i_day in range(8):
            for i_hour in range(3):
                time_string=the_day.strftime("%Y%m%d")
                
                url_time_string=time_string+str(i_day*3).zfill(2)+"_"+str(i_hour).zfill(3)+"_"
                url = dwd_path+str(i_day*3).zfill(2)+"/"+weather_variable+"/"+simulation_name+url_time_string+field_name+file_ending
                
                destination_time_string=time_string+str(i_day*3+i_hour).zfill(2)+"_"
                destination = os.path.join(destination_folder, simulation_name+"_"+destination_time_string+field_name+file_ending)
                
                file_name_string=simulation_name+url_time_string+field_name+file_ending
                
                if os.path.isfile(destination):
                    print("Skip: "+file_name_string)
                    #print("Dest"+destination)
                else:
                    try:
                        print("Download: "+url)
                        wget.download(url, out=destination)
                        print(" ")
                        print(" to..."+destination)
                        time.sleep(0.5)
                    except:
                        print(url+" does not exist on DWD opendata")

##Clean code to download 1 variable
weather_variables=["asob_s"]

for weather_variable in weather_variables:
    field_name="2d_"+weather_variable
    cycle_time=[start_day,end_day]
    for the_day in cycle_time:
        for i_day in range(8):
            for i_hour in range(1):
                step_3hour_average=3
                time_string=the_day.strftime("%Y%m%d")
                
                url_time_string=time_string+str(i_day*3).zfill(2)+"_"+str(i_hour+step_3hour_average).zfill(3)+"_"
                url = dwd_path+str(i_day*3).zfill(2)+"/"+weather_variable+"/"+simulation_name+url_time_string+field_name+file_ending
                
                destination_time_string=time_string+str(i_day*3+i_hour+step_3hour_average).zfill(2)+"_"
                destination = os.path.join(destination_folder, simulation_name+"_3h_average_"+destination_time_string+field_name+file_ending)
                
                file_name_string=simulation_name+url_time_string+field_name+file_ending
                
                if os.path.isfile(destination):
                    print("Skip: "+file_name_string)
                    #print("Dest"+destination)
                else:
                    try:
                        print("Download: "+url)
                        wget.download(url, out=destination)
                        print(" ")
                        print(" to..."+destination)
                        time.sleep(0.5)
                    except:
                        print(url+" does not exist on DWD opendata")

##Clean code to download 1 variable
weather_variables=["tot_prec"]

for weather_variable in weather_variables:
    field_name="2d_"+weather_variable
    cycle_time=[start_day,end_day]
    for the_day in cycle_time:
        for i_day in range(8):
            for i_hour in range(3):
                step_3hour_average=3
                time_string=the_day.strftime("%Y%m%d")
                
                url_time_string=time_string+str(i_day*3).zfill(2)+"_"+str(i_hour+1).zfill(3)+"_"
                url = dwd_path+str(i_day*3).zfill(2)+"/"+weather_variable+"/"+simulation_name+url_time_string+field_name+file_ending
                
                destination_time_string=time_string+str(i_day*3+i_hour+1).zfill(2)+"_"
                destination = os.path.join(destination_folder, simulation_name+"_3h_accumulated_"+destination_time_string+field_name+file_ending)
                
                file_name_string=simulation_name+url_time_string+field_name+file_ending
                
                if os.path.isfile(destination):
                    print("Skip: "+file_name_string)
                    #print("Dest"+destination)
                else:
                    try:
                        print("Download: "+url)
                        wget.download(url, out=destination)
                        print(" ")
                        print(" to..."+destination)
                        time.sleep(0.5)
                    except:
                        print(url+" does not exist on DWD opendata")
print("block completed")