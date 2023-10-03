import os
import shutil
from urllib.request import urlopen # <------
from datetime import datetime
import hashlib
from collections import defaultdict
import re
import pickle
import ssl
import pandas as pd
import io
import requests
import json

class rt_revision_tracker():
    
    """
    Track & extracts revisions made to UFS weather model's development branch data timestamps.
    
    """
    
    def __init__(self):
        
        # Results root foldername. 
        self.latest_results_root =  './track_ts/'
        
        # Directory of historical results.
        self.history_results_dir = f'{self.latest_results_root}historical_updates/'
        
        # Filename of latest files' (rt.sh & bl_date.conf) results. # *************** CHANGED 8/8
        self.latest_results_fn = 'latest_rt.sh'
        self.latest_bl_results_fn = 'latest_bl_date.conf' # ********************************************
        
        # If folder does not exist, create folder
        if not os.path.exists(self.latest_results_root):
            os.makedirs(self.latest_results_root)
            print(f"\033[1mCreated Latest Results Directory: {self.latest_results_root}\033[0m")
        if not os.path.exists(self.history_results_dir):
            os.makedirs(self.history_results_dir)
            print(f"\033[1mCreated Historical Results Directory: {self.history_results_dir}\033[0m")
            
        # File of interest's source.
        self.url = 'https://github.com/ufs-community/ufs-weather-model/blob/develop/tests/rt.sh'
        self.url_bl = 'https://github.com/ufs-community/ufs-weather-model/blob/develop/tests/bl_date.conf'  
        
    def parser(self, fn, bl_fn, data_log_dict):
        """
        
        Parse & extract timestamps from the UFS weather model's development branch.
        
        Detects 8-digits & relevant variable names for which are setting the timestamps.

        Args:
            fn (str): Directory to retrieved file comprised of the non-baseline data timestamp.
            bl_fn (str): Directory to retrieved file comprised of the baseline data timestamp.
            data_log_dict (dict): Dictionary of the previous file.

        Return (dict): Updated dictionary comprised of new updates made to file. 
        
        
        """

        # Relevant variable names for which are setting the timestamps.
        ts_vars = {"bl_vars": "BL_DATE",
                   "input_root_vars": "INPUTDATA_ROOT",
                   "input_ww3_vars": "INPUTDATA_ROOT_WW3",
                   "input_bmic_vars": "INPUTDATA_ROOT_BMIC"}
        inputdataroot_embedded = ["INPUTDATA_ROOT_WW3", "INPUTDATA_ROOT_BMIC"]

        # Record date of retrieving updated file.
        # Datetime object containing current date and time
        retrieval_dt = datetime.now() 
        dt_str = str(retrieval_dt.strftime("%m-%d-%Y"))
        
        # Create key for new date if does not exist in dictionary.
        if dt_str not in data_log_dict:
            data_log_dict[dt_str] = {ts_vars["bl_vars"]: [],
                                     ts_vars["input_root_vars"]: [],
                                     ts_vars["input_ww3_vars"]: [],
                                     ts_vars["input_bmic_vars"]: []}
            
        # Confirm the variables setting timestamps.
        data_log = []
        with open(fn, 'r') as f:
            data = f.readlines()
            for line in data:
                if ts_vars["input_root_vars"] in line and re.findall(r'[0-9]{8}', line):
                    input_root_var = list(set(re.findall(r'\bINPUTDATA_ROOT\b', line)))
                    input_root_ts = re.findall(r'input-data-[0-9]{8}', line)
                    input_ts_numerics = re.findall(r'\d+', str(input_root_ts))
                    data_log.append(input_root_var + input_ts_numerics)
                    if input_ts_numerics[0] not in data_log_dict[dt_str][input_root_var[0]]:
                        data_log_dict[dt_str][input_root_var[0]].append(input_ts_numerics[0])
                    else:
                        pass

                if ts_vars["input_ww3_vars"] in line and re.findall(r'[0-9]{8}', line):
                    ww3_input_var = list(set(re.findall(r'\bINPUTDATA_ROOT_WW3\b', line)))
                    ww3_input_ts = re.findall(r'input_data_[0-9]{8}', line)
                    ww3_input_ts_numerics = re.findall(r'\d+', str(ww3_input_ts))
                    data_log.append(ww3_input_var + ww3_input_ts_numerics)
                    if ww3_input_ts_numerics[0] not in data_log_dict[dt_str][ww3_input_var[0]]:
                        data_log_dict[dt_str][ww3_input_var[0]].append(ww3_input_ts_numerics[0])
                    else:
                        pass

                if ts_vars["input_bmic_vars"] in line and re.findall(r'[0-9]{8}', line):
                    bmic_input_var = list(set(re.findall(r'\bINPUTDATA_ROOT_BMIC\b', line)))
                    bmic_input_ts = re.findall(r'IC-[0-9]{8}', line)
                    bmic_input_ts_numerics = re.findall(r'\d+', str(bmic_input_ts))
                    data_log.append(bmic_input_var + bmic_input_ts_numerics)
                    if bmic_input_ts_numerics[0] not in data_log_dict[dt_str][bmic_input_var[0]]:
                        data_log_dict[dt_str][bmic_input_var[0]].append(bmic_input_ts_numerics[0])

                    else:
                        pass
                    
        with open(bl_fn, 'r') as f2: ### ********************************* ADDED 07/28
            data_bl = f2.readlines()
            for line in data_bl:
                if ts_vars["bl_vars"] in line and re.findall(r'[0-9]{8}', line):
                    bl_var = list(set(re.findall(r'\bBL_DATE\b', line)))
                    bl_ts = re.findall(r'[0-9]{8}', line)
                    data_log.append(bl_var + bl_ts)
                    if bl_ts[0] not in data_log_dict[dt_str][bl_var[0]]:
                        data_log_dict[dt_str][bl_var[0]].append(bl_ts[0])
                    else:
                        pass
                                        
        # Save parsed results of retrieved file (.sh).         
        with open(fn, 'w') as f:
            f.write(str(dict(data_log_dict)))
            
        # Save parsed results of retrieved file (.conf) for baseline dataset  
        with open(bl_fn, 'w') as f_bl:
            f_bl.write(str(dict(data_log_dict)))
            
        # Save parsed results of retrieved file (.pk) serialize lambdas and defaultdicts via dill.
        with open(fn + '.pk', 'wb') as pk_handle:
            pickle.dump(dict(data_log_dict), pk_handle, protocol=pickle.HIGHEST_PROTOCOL)
        os.rename(fn + '.pk', self.latest_results_root + self.latest_results_fn + '.pk')   

        return data_log_dict ### *******************************************

    def sha1(self, fn):
        """
        Calculate the retrieved file's hash.
        
        Args:
            fn (str): Directory of file to calculate its hash.

        Return (str): Retrieved file's calculated hash.
        

        """

        # Calculate retrieved file's SHA.
        BUF_SIZE = 65536
        sha1 = hashlib.sha1()
        with open(fn, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)
                
        return sha1.hexdigest()

    def move_files(self, retrieved_fn):
        """
        Allocate retrieved file to historical log folder & renames it as the latest file.

        Args: 
            retrieved_fn (str): Directory of retrieved file.

        Return: None
        

        """
        
        # Allocate retrieved file to historical log folder.
        shutil.copy(retrieved_fn, self.history_results_dir)

        # Rename retrieved file as latest file.
        os.rename(retrieved_fn, 
                  self.latest_results_root + self.latest_results_fn)

        return

    def check_for_update(self, data_log_dict): # ******************** CHANGED ENTIRELY
        """
        Update the latest file if a revision was made since the last time of retrieval.

        Args:
            data_log_dict(dict): Dictionary of the previous file.

        Return (dict): Updated dictionary comprised of new updates made to file. 
        

        """ 
        # Read/write file comprised of non-baseline datasets to disk. # ******************************************** CHANGED 07/28
        # INITIAL APPROACH
#         data_bytes = urlopen(self.url, context=ssl.SSLContext()).read()
#         retrieved_results_fn = '{}_rt.sh'.format(datetime.now().strftime("%m-%d-%Y"))        
#         with open(self.latest_results_root + retrieved_results_fn, 'w') as raw_bytes_file:
#             raw_bytes_file.write(data_bytes.decode('utf-8'))
#             #print('##### *******', data_bytes.decode('utf-8'))
#         print('\033[94m\033[1m\nRetrieved rt.sh saved as latest rt.sh version...\033[0m')

        # Read/write file comprised of non-baseline datasets to disk. # ******************************************** CHANGED 07/28
        data_bytes = requests.get(self.url).content
        data_bytes = json.loads(data_bytes.decode('utf-8'))
        retrieved_results_fn = '{}_rt.sh'.format(datetime.now().strftime("%m-%d-%Y"))        
        with open(self.latest_results_root + retrieved_results_fn, 'w') as raw_bytes_file:
            raw_bytes_file.write(str(data_bytes['payload']['blob']))
            #print('##### *******', data_bytes['payload']['blob'])
        print('\033[94m\033[1m\nRetrieved rt.sh saved as latest rt.sh version...\033[0m')
        
        # Read/write file comprised of baseline dataset to disk.
        data_bytes_bl = requests.get(self.url_bl).content
        data_bytes_bl = json.loads(data_bytes_bl.decode('utf-8'))
        retrieved_results_bl_fn= '{}_bl_date.conf'.format(datetime.now().strftime("%m-%d-%Y"))
        with open(self.latest_results_root + retrieved_results_bl_fn, 'w') as raw_bytes_file_bl:
            raw_bytes_file_bl.write(str(data_bytes_bl['payload']['blob']['rawLines']))
            #print('##### *******', data_bytes_bl['payload']['blob']['rawLines'])
        print('\033[94m\033[1m\nRetrieved bl_date.conf saved as latest bl_date.conf version...\033[0m')
            
        # Parse retrieved file.************************************************ CHANGED 07/28
        data_log_dict = self.parser(self.latest_results_root + retrieved_results_fn, self.latest_results_root + retrieved_results_bl_fn, data_log_dict)
        
        try:

            # Calculate hash of latest bl_date.conf & rt.sh files -- if still exist. # *************** CHANGED
            hash_latest_bl = self.sha1(self.latest_results_root + self.latest_bl_results_fn)
            hash_latest = self.sha1(self.latest_results_root + self.latest_results_fn)
            print("\033[1mLatest bl_date.conf file's hash:\033[0m", hash_latest_bl)
            print("\033[1mLatest rt.sh file's hash:\033[0m", hash_latest)
            
        except:
            
            # Transfer parsed file from results root's folder to historical log folder.
            # & rename initial file as the latest file. 
            self.move_files(self.latest_results_root + retrieved_results_bl_fn)
            print('\033[1m\nRetrieved file saved as initial bl_date.conf version...\033[0m\n')
            
            self.move_files(self.latest_results_root + retrieved_results_fn)
            print('\033[1m\nRetrieved file saved as initial rt.sh version...\033[0m\n')

            return data_log_dict 

        # Calculate hash of bl_date.conf file.
        hash_new_bl = self.sha1(self.latest_results_root + retrieved_results_bl_fn)
        print("\033[1mRetrieved bl_date.conf file's hash:\033[0m", hash_new_bl)

        # Save retrieved bl_date.conf to historical log folder only if commited revisions/updates were made.
        if hash_latest_bl != hash_new_bl:
            
            # Save updated file as the new latest_bl_date.conf file.
            print('\033[1m\033[92mUpdates were made to baseline data timestamps within file since last retrieved file.\033[0m')
            self.move_files(self.latest_results_root + retrieved_results_bl_fn)
            
            # [Optional] Could add a email notification feature here. => Email feature was added to environment in Jenkins
            
        else:

            # Removes retrieved file from results root's folder since no updates were made.
            print('\033[1m\033[91mNo updates were made to baseline data timestamps within file since last retrieved file.\033[0m')
            os.remove(self.latest_results_root + retrieved_results_bl_fn)

        # Calculate hash of rt.sh file.
        hash_new = self.sha1(self.latest_results_root + retrieved_results_fn)
        print("\033[1mRetrieved rt.sh file's hash:\033[0m", hash_new)
        
        # Save retrieved rt.sh file to historical log folder only if commited revisions/updates were made.
        if hash_latest != hash_new:
            
            # Save updated file as the new latest_rt.sh file.
            print('\033[1m\033[92mUpdates were made to non-baseline data timestamps within file since last retrieved file.\033[0m')
            self.move_files(self.latest_results_root + retrieved_results_fn)
            
            # [Optional] Could add a email notification feature here. => Email feature was added to environment in Jenkins
            
        else:

            # Removes retrieved file from results root's folder since no updates were made.
            print('\033[1m\033[91mNo updates were made to non-baseline data timestamps within file since last retrieved file.\033[0m')
            os.remove(self.latest_results_root + retrieved_results_fn)
            
        return data_log_dict # **********************************************************************

    def reset_tracker(self):
        """
        
        Restart the accumulation of timestamps.

        Args: 
            None

        Return: None

            Acts as a reset button for when one would like to restart the accumulation of the timestamps.
        Jenkins will trigger every N months. Every two months only the latest log file will be reset while 
        all file updates will be seen w/in the historical folder. Must initialize first for an initial
        file prior to populating newly updated files.
        

        """
        data_log_dict = self.check_for_update(defaultdict(lambda: defaultdict(list)))
        print('\033[94m' + '\033[1m' + f'\nInitial Timestamps at Restart:\033[0m\033[1m\n{data_log_dict}\033[0m')

        return

    def populate(self):
        """
        Accumulation of timestamps since time of reset.

        Args: 
            None

        Return: None
        
            Jenkins will call method for every Github push pertaining to rt.sh it detects.

        """
        
        # Recall latest updated file & populate w/ retrieved file if it has a timestamp revision.
        with open(f"{self.latest_results_root}{self.latest_results_fn}.pk", 'rb') as handle:
            data_log_dict = pickle.load(handle)                
        print('\033[94m' + '\033[1m' + f'\nTimestamps (Prior to File Retrieval):\033[0m\033[1m\n{data_log_dict}\033[0m')    

        data_log_dict = self.check_for_update(data_log_dict)  
        print('\033[94m' + '\033[1m' + f'\nPopulated Timestamps (After Latest Newly Committed PR is Pushed to GitHub Repository of Interest):\033[0m\033[1m\n{data_log_dict}\033[0m')

        return data_log_dict 
