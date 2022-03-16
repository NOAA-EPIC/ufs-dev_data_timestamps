import os
import shutil
from urllib.request import urlopen
from datetime import datetime
import hashlib
from collections import defaultdict
import re
import pickle
import ssl

class rt_revision_tracker():
    
    """
    Track & extracts revisions made to UFS weather model's development branch data timestamps.
    
    """
    
    def __init__(self):
        
        # Results root foldername. 
        self.latest_results_root =  './track_ts/'
        
        # Directory of historical results.
        self.history_results_dir = f'{self.latest_results_root}historical_updates/'
        
        # Filename of latest file's results.
        self.latest_results_fn = 'latest_rt.sh'
        
        # If folder does not exist, create folder
        if not os.path.exists(self.latest_results_root):
            os.makedirs(self.latest_results_root)
            print(f"Created Latest Results Directory: {self.latest_results_root}")
        if not os.path.exists(self.history_results_dir):
            os.makedirs(self.history_results_dir)
            print(f"Created Historical Results Directory: {self.history_results_dir}")
            
        # File of interest's source.
        #self.url = 'https://github.com/ufs-community/ufs-weather-model/blob/develop/tests/rt.sh'
        
        # File of interest's source.(For Testing Purposes)
        self.url = 'https://github.com/NOAA-EPIC/ufs-weather-model/blob/test-develop/tests/rt.sh'
        
    def parser(self, fn, data_log_dict):
        """
        
        Parse & extract timestamps from the UFS weather model's development branch.
        
        Detects 8-digits & relevant variable names for which are setting the timestamps.

        Args:
            fn (str): Directory to retrieved file.
            data_log_dict (dict): Dictionary of the previous file.

        Return (dict): Updated dictionary comprised of new updates made to file. 
        
        
        """
        # Relevant variable names for which are setting the timestamps.
        ts_vars = {"bl_vars": "BL_DATE",
                   "input_root_vars": "INPUTDATA_ROOT",
                   "input_ww3_vars": "INPUTDATA_ROOT_WW3",
                   "input_bmic_vars": "INPUTDATA_ROOT_BMIC"}

        inputdataroot_embedded = ["INPUTDATA_ROOT_WW3", "INPUTDATA_ROOT_BMIC"]

        data_log = []

        with open(fn, 'r') as f:
            data = f.readlines()
            for line in data:
                if ts_vars["bl_vars"] in line and re.findall(r'[0-9]{8}', line):
                    bl_ts = re.findall(r'[0-9]{8}', line)
                    bl_var = list(set(re.findall(r'\bBL_DATE\b', line)))
                    data_log.append(bl_var + bl_ts)
                    if bl_ts[0] not in data_log_dict[bl_var[0]]:
                        data_log_dict[bl_var[0]].append(bl_ts[0])
                    else:
                        pass

                if ts_vars["input_root_vars"] in line and all(x not in line for x in inputdataroot_embedded) and re.findall(r'[0-9]{8}', line):
                    input_root_ts = re.findall(r'[0-9]{8}', line)
                    input_root_var = list(set(re.findall(r'\bINPUTDATA_ROOT\b', line)))
                    data_log.append(input_root_var + input_root_ts)
                    if input_root_ts[0] not in data_log_dict[input_root_var[0]]:
                        data_log_dict[input_root_var[0]].append(input_root_ts[0])
                    else:
                        pass

                if ts_vars["input_ww3_vars"] in line and re.findall(r'[0-9]{8}', line):
                    ww3_input_ts = re.findall(r'[0-9]{8}', line)
                    ww3_input_var = list(set(re.findall(r'\bINPUTDATA_ROOT_WW3\b', line)))
                    data_log.append(ww3_input_var + ww3_input_ts)
                    if ww3_input_ts[0] not in data_log_dict[ww3_input_var[0]]:
                        data_log_dict[ww3_input_var[0]].append(ww3_input_ts[0])
                    else:
                        pass

                if ts_vars["input_bmic_vars"] in line and re.findall(r'[0-9]{8}', line):
                    bmic_input_ts = re.findall(r'[0-9]{8}', line)
                    bmic_input_var = list(set(re.findall(r'\bINPUTDATA_ROOT_BMIC\b', line)))
                    data_log.append(bmic_input_var + bmic_input_ts)
                    if bmic_input_ts[0] not in data_log_dict[bmic_input_var[0]]:
                        data_log_dict[bmic_input_var[0]].append(bmic_input_ts[0])
                    else:
                        pass

        # Save parsed results of retrieved file (.sh).         
        with open(fn, 'w') as f:
            f.write(str(data_log_dict))

        ## Save parsed results of retrieved file (.pk).
        with open(fn + '.pk', 'wb') as pk_handle:
            pickle.dump(data_log_dict, pk_handle, protocol=pickle.HIGHEST_PROTOCOL)
        os.rename(fn + '.pk', self.latest_results_root + self.latest_results_fn + '.pk')   

        return data_log_dict

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

    def check_for_update(self, data_log_dict):
        """
        Update the latest file if a revision was made since the last time of retrieval.

        Args:
            data_log_dict(dict): Dictionary of the previous file.

        Return (dict): Updated dictionary comprised of new updates made to file. 
        

        """ 
        
        # Read retrieved file.
        data_bytes = urlopen(self.url, context=ssl.SSLContext()).read()
        retrieved_results_fn = '{}_rt.sh'.format(datetime.now().strftime("%Y%d%m%H%M%S"))
        with open(self.latest_results_root + retrieved_results_fn, 'w') as raw_bytes_file:
            raw_bytes_file.write(data_bytes.decode('utf-8'))
        print('\nRetrieved file saved as latest rt.sh version.')
            
        # Parse retrieved file.
        data_log_dict = self.parser(self.latest_results_root + retrieved_results_fn, data_log_dict)
        try:

            # Calculate hash of latest file -- if exist.
            hash_latest = self.sha1(self.latest_results_root + self.latest_results_fn)
            print("Latest rt.sh file's hash:", hash_latest)
            
        except:
            
            # Transfer parsed file from results root's folder to historical log folder.
            # & rename initial file as the latest file. 
            self.move_files(self.latest_results_root + retrieved_results_fn)
            print('\nRetrieved file saved as initial rt.sh version.')

            return data_log_dict 

        # Calculate hash of retrieved file.
        hash_new = self.sha1(self.latest_results_root + retrieved_results_fn)
        print("Retrieved file's hash:", hash_new)

        # Save retrieved file to historical log folder only if commited revisions/updates were made.
        if hash_latest != hash_new:
            print('Updates were made to data timestamps within file since last retrieved file.')
            self.move_files(self.latest_results_root + retrieved_results_fn)
            
            # [Optional] Could add a email notification feature here.
            
        else:

            # Removes retrieved file from results root's folder since no updates were made.
            print('No updates were made to data timestamps within file since last retrieved file.')
            os.remove(self.latest_results_root + retrieved_results_fn)

        return data_log_dict

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
        data_log_dict = self.check_for_update(defaultdict(list))

        print(f'\nInitial Timestamps at Restart:\n{data_log_dict}')

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
        print('\nTimestamps (Prior to File Retrieval):\n', data_log_dict)    
        data_log_dict = self.check_for_update(data_log_dict)  
        print(f'\nPopulated Timestamps (After a New GitHub Push Was Made):\n{data_log_dict}')

        return
