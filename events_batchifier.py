def logger(msg):
    # print(msg)
    log_file.write(msg+"\n")
    print(msg+"\n")

def zipper(zip_path,nft_files):
    with zipfile.ZipFile(zip_path, 'x') as zipMe:
        for file in nft_files:
            absname = os.path.abspath(os.path.join(path+folder, file))
            arcname = os.path.basename(absname)
            zipMe.write(file, compress_type=zipfile.ZIP_DEFLATED, arcname=arcname)
            msg ="zipped "+ arcname+" to "+os.path.basename(zip_path)
            logger(msg)
    msg = "Successfully zipped "+zip_path+"\n"
    logger(msg)

# for i in range(2,7):
import zipfile
import os
from datetime import date
from csv_batch_creator import create_batch
import time

t0 = time.time()
path = r'C:\Users\atish\Documents\GitHub\NFT_thesis\events' # use your path

log_file = open(path+'\error_log_zipper.txt',"a")
fucked = False
# folder = "\\" + str(i)
folder = ''
# print(i)

try:
    csv_batch_log_list = create_batch(path+folder) #create a batch csv file of all the events downloaded for that day
except Exception as e:
    logger(str(e))
    fucked = True
    # break

nft_files = csv_batch_log_list[1]
nft_files.append(csv_batch_log_list[2])
today = str(date.today())
file_name = 'nft_events_batch_'+today+'.zip'
zip_path = os.path.join(path+"\half_year\sampled",file_name)



csv_batch_log = "\n".join(csv_batch_log_list[0])
logger("Run date: "+today)
logger(csv_batch_log)

try:
    if not os.path.exists(zip_path):
        zipper(zip_path,nft_files)
    else:
        i = 0
        while True:
            file_name = 'nft_events_batch_'+today+"("+str(i)+').zip'
            zip_path = os.path.join(path+"\half_year\sampled",file_name)
            if not os.path.exists(zip_path):
                break
            i+=1
        zipper(zip_path,nft_files)
        i+=1
except Exception as e:
    logger(str(e))
    fucked = True
    # break
    
if not fucked:
    try:
        for file_rem in nft_files:
            absname = os.path.abspath(os.path.join(path+folder, file_rem))
            arcname = os.path.basename(absname)
            msg = "removing "+arcname
            logger(msg)
            os.remove(file_rem)
    except Exception as e:
        logger(str(e))
        # break

t1 = time.time()
logger("Saving "+file_name+"\nFinished in "+str(round(t1-t0))+" seconds\n-----\n")
log_file.close()
