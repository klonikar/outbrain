#############################################################
## KRISHNA KULKARNI                                        ##
## krishna.kulkarni@cern.ch                                ##
#############################################################
## Accessing number of rows in file                        ##
## creating sample file                                    ##
## accessing data in array and print it and its shape      ##
## accessing all .csv files                                ##
## run script with multiprocessing parallelly              ##
#############################################################


###############importing library###########
import numpy as np # linear algebra
#import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import os
#import gc # We're gonna be clearing memory a lot
#import matplotlib.pyplot as plt
#import seaborn as sns
#matplotlib inline
import csv
#import multiprocessing
from collections import defaultdict
###########################################

#p = sns.color_palette()

#########################counting rows in file##########
#def row_count(myfile,b):
#    index = multiprocessing.current_process()._identity[0]
#    workername = multiprocessing.current_process().name
#    print "worker name ", workername, "and his identity number ", index
#    temp_file=csv.reader(open('/nfs/dust/atlas/user/kulkarnk/myproject/outbrain/input/' + myfile,'rb'))
#    rows_in_file=sum(1 for row in temp_file)
#    print 'rows in file : ', myfile, 'are : ', rows_in_file
########################################################


################## Counting ##########################
#uuid_temp=defaultdict(int)



########################creating multiprocessing pool###################
#PROCESSES = 5
#pool = multiprocessing.Pool(PROCESSES)
#print 'pool = %s' % pool
#print('# File sizes')
###################################################################


########################### breaking .csv file##############################
#i=0
#sample_file = open("/nfs/dust/atlas/user/kulkarnk/myproject/outbrain/input/page_views/page_views"+str(i/20000000)+".csv", "wb")
#sample_file_object = csv.writer(sample_file)
#header=clicks_train.next()
#sample_file_object.writerow(header)
#for row in clicks_train:
#   # data.append(row)
#    i=i+1
#    if i%20000000 != 0:
#      #  data=np.array(data)
#        sample_file_object.writerow(row)
#    else:
#        sample_file = open("/nfs/dust/atlas/user/kulkarnk/myproject/outbrain/input/page_views/page_views"+str(i/20000000)+".csv", "wb")
#        sample_file_object = csv.writer(sample_file)
#        sample_file_object.writerow(header)
#
#        print 'creating new file...'
#        print 'loading data...'
###########################################################################


####################loop operating on all .csv files#######
#for f in os.listdir('/nfs/dust/atlas/user/kulkarnk/myproject/outbrain/input'):
#    if 'zip' not in f:
#        print(f.ljust(30) + str(round(os.path.getsize('/nfs/dust/atlas/user/kulkarnk/myproject/outbrain/input/' + f) / 1000000, 2)) + 'MB#')
#        pool.apply_async(func = row_count, args = (f,3))
##########################################################


#################opening file##########
#INPUTFILE="!INPUTFILE!"
#df_train=csv.reader(open(INPUTFILE,'rb'))
df_train=csv.reader(open('/nfs/dust/atlas/user/kulkarnk/myproject/outbrain/input/clicks_train.csv','rb'))
#df_train=csv.reader(open('/nfs/dust/atlas/user/kulkarnk/myproject/outbrain/input/clicks_train_sample.csv','rb'))

#######################################

###########accessing header of file######
header=df_train.next()
######################################

##############creating data array#####
j=0
data=[]

for row in df_train:
    data.append(row)
    j=j+1
    if j%6000000 == 0:
        print 'appending array... : ',j
data=np.array(data)
#print data[0,1] #Ad_ID
#data[0:,2:3] #Output
#####################################
click_count=defaultdict(int)
ad_count=defaultdict(int)
#print data[0:,1:3]
j=0
for row in data[0:,1:3]:
    click_count[row[0]] += int(row[1])
    ad_count[row[0]] += 1
    j=j+1
    if j%6000000 == 0:
        print 'counting ad multiplicity... : ',j


j=0
del data
click_count_arr=[]
ad_id_arr=[]
for x,y in click_count.items():
    click_count_arr.append(float(y))
    ad_id_arr.append(int(x))
    j=j+1
    if j%6000000 == 0:
        print 'merging array... : ',j


#print click_count_arr
#print ad_id_arr

ad_count_arr=[]
for z,w in ad_count.items():
    ad_count_arr.append(float(w))
#print click_count_arr[1]
#print np.shape(click_count_arr)
#print np.shape(ad_count_arr)
combined_array=[]
#print np.concatenate((click_count_arr, ad_count_arr.T), axis=1)
combined_array=np.vstack((click_count_arr, ad_count_arr,ad_id_arr))
del click_count_arr
del ad_count_arr

combined_array=combined_array.T
print combined_array
total_ads=0
zero_clicks=0
twenty_percent_clicks=0
fifty_percent_clicks=0
seventy_percent_clicks=0
less_than_hundred_percent_clicks=0
hundred_percent_clicks=0
normal_click_count_arr=[]
j=0
for row2 in combined_array:
    normal_click_count_arr.append(float(row2[0]/row2[1]))
    total_ads=total_ads+1
    j=j+1
    if j%6000000 == 0:
        print 'dividing into catagories... : ',j

    if row2[0] == 0:
        zero_clicks=zero_clicks+1
    if row2[0] != 0 and row2[0]/row2[1] < 0.2:
        twenty_percent_clicks=twenty_percent_clicks+1
    if row2[0]/row2[1] > 0.2 and row2[0]/row2[1] < 0.5:
        fifty_percent_clicks=fifty_percent_clicks+1
    if row2[0]/row2[1] > 0.5 and row2[0]/row2[1] < 0.7:
        seventy_percent_clicks=seventy_percent_clicks+1
    if row2[0]/row2[1] > 0.7 and row2[0]/row2[1] < 1.0:
        less_than_hundred_percent_clicks=less_than_hundred_percent_clicks+1
    if row2[0]/row2[1] == 1.0:
        hundred_percent_clicks=hundred_percent_clicks+1
    #    print row2[0]/row2[1]
combined_normal_click_count_arr=np.vstack((ad_id_arr,normal_click_count_arr))
del ad_id_arr
del normal_click_count_arr

combined_normal_click_count_arr=combined_normal_click_count_arr.T
print combined_normal_click_count_arr
#print normal_click_count_arr
#print 'normal_click_count_arr : ', np.shape(normal_click_count_arr)
#print INPUTFILE
print 'total ads : ',total_ads
print 'zero_clicks : ',zero_clicks
print 'more than zero and less_than_twenty_percent_times_clicks : ',twenty_percent_clicks
print 'more than twenty_percent_times and less_than_fifty_percent_times_clicks : ',fifty_percent_clicks
print 'more than fifty_percent_times and less_than_seventy_percent_times_clicks : ',seventy_percent_clicks
print 'more than sevent_percent_times and less_than_hundred_percent_times_clicks : ',less_than_hundred_percent_clicks
print 'hundred_percent_clicks : ',hundred_percent_clicks
####################################################################
#df_train=csv.reader(open('/nfs/dust/atlas/user/kulkarnk/myproject/outbrain/input/clicks_test.csv','rb'))

sample_file = open("/nfs/dust/atlas/user/kulkarnk/myproject/outbrain/output/firstmodel/normal_ads_clicked.csv", "wb")


sample_file_object = csv.writer(sample_file)
for row3 in combined_normal_click_count_arr:
    sample_file_object.writerow(row3)


####################################################################
#string1=INPUTFILE
#string2='total ads : ',total_ads
#string3='zero_clicks : ',zero_clicks
#string4='more than zero and less_than_twenty_percent_times_clicks : ',twenty_percent_clicks
#string5='more than twenty_percent_times and less_than_fifty_percent_times_clicks : ',fifty_percent_clicks
#string6='more than fifty_percent_times and less_than_seventy_percent_times_clicks : ',seventy_percent_clicks
#string7='more than sevent_percent_times and less_than_hundred_percent_times_clicks : ',less_than_hundred_percent_clicks
#string8='hundred_percent_clicks : ',hundred_percent_clicks
#sample_file = open("/afs/desy.de/user/k/kulkarnk/private/myproj/kaggle/outbrain/models/29Oct2016/output.log", "wb")
#sample_file_object = csv.writer(sample_file)
#sample_file_object.writerow(string1)
#sample_file_object.writerow(string2)
#sample_file_object.writerow(string3)
#sample_file_object.writerow(string4)
#sample_file_object.writerow(string5)
#sample_file_object.writerow(string6)
#sample_file_object.writerow(string7)
#sample_file_object.writerow(string8)

############creating sample file#######
#data=[]
#    print 'appending data :',  j
#    j=j+1
#    while j<=20000:
#        sample_file_object.writerow(row)
#        print 'writing in sample file : ', row
#        break
#    if j>20000:
#        break
#####################################




###########printing data#############
#print 'size of data : ', np.size(data)
#print data
#print np.shape(data)
####################################


#df_train.close()
#sample_file.close()


#pool.close()
#pool.join()
