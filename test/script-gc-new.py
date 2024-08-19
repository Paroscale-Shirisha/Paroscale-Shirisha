import subprocess
import json
import sys
import time
import random
import uuid
import os
import signal
import shutil
import re

custom_path_nisd = sys.argv[1]
custom_path_gc = sys.argv[2]
config_path =  sys.argv[3]
num_vdev = int(sys.argv[4])
num_chunks = int(sys.argv[5])
ss_value = 0
def_uuid = uuid.uuid4()
chunk = 0
seed = 123
end_seq = 0
bs = 0
bsm = 0
mp = 0
mv = 0
pa = 0
pp = 0
ps = 0
t = 0
va = 0
vp = 0
vs = 0
Data = 0
checkmarker = 0
GCProcess_Pid = 0
result_array = []
failed_chunk = []
vdev_array =[]

j_values_datavalidation = []
j_values_dummygenerator = []
stdout_file = "Gc.log"
stderr_file = "GCerr.log"

# Define process globally
process = None

# Start the GCservice here using subprocess.open it has to be run in back-ground.
#command = f"./GCService -path {custom_path_gc} -s3config {config_path} -s3log {custom_path_gc}/GCs3log"
command = f"./GCService -path {custom_path_gc} -s3config {config_path} -s3log {custom_path_gc}/GCs3log"
with open(stdout_file, "w") as stdout, open(stderr_file, "w") as stderr:
    process = subprocess.Popen(command, shell=True, stdout=stdout, stderr=stderr)
    GCProcess_Pid = process.pid
#process = subprocess.Popen(command, shell=True)

if process is None:
    print("Error: Failed to start GCService process.")
    sys.exit(1)

#for vdev in range(num_vdev):
#uuid1 = uuid.uuid4()
uuid1 = "10066170-c652-11ee-a2e9-22abb648e432"
vdev_array.append(uuid1)
uuid2 = "1f758d48-c652-11ee-997e-22abb648e432"
vdev_array.append(uuid2)

print("vdev array " , vdev_array)

def find_and_kill_process(keyword):
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmdline = proc.cmdline()
            if any(keyword in arg for arg in cmdline):
                pid = proc.pid
                print(f"Process found with PID {pid}. Killing it...")
                os.kill(pid, 9)  # Send SIGKILL signal to terminate the process
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

def remove_old_folders(directory, prefix, suffix_pattern):
    latest_folder = None
    latest_mtime = 0

    # Iterate over all folders in the directory
    for folder in os.listdir(directory):
        # Check if the folder name starts with the specified prefix
        if folder.startswith(prefix):
            # Check if the folder name matches the suffix pattern
            if re.search(suffix_pattern, folder):
                # Construct the full path to the folder
                folder_path = os.path.join(directory, folder)
                # Get the modification time of the folder
                mtime = os.path.getmtime(folder_path)
                # Update latest_folder if the current folder is newer
                if mtime > latest_mtime:
                    latest_folder = folder
                    latest_mtime = mtime

    # Remove all folders except the latest one
    for folder in os.listdir(directory):
        if folder.startswith(prefix) and folder != latest_folder and re.search(suffix_pattern, folder):
            folder_path = os.path.join(directory, folder)
            # Remove the folder and all its contents recursively
            shutil.rmtree(folder_path)
            print(f"Removed old folder: {folder_path}")


def generate_new_command(newchunk, seed, result_array, vdev, endSeq, GCProcess_Pid):
    seed = random.randint(1, 10000)
    if newchunk not in result_array:
        result_array.append(newchunk)

    bsm = random.randint(5, 32)
    mp = random.randint(10000, 500000)
    mv = random.randint(10024, 655536)
    pa = random.randint(10001, 655536)
    ps = random.randint(5000, 500000)
    t = random.randint(0, 4)
    va = random.randint(1000000, 2000000)
    vp = random.randint(1000, 10000)
    vs = -1
    stdoutdg_file = "DG.log"
    stderrdg_file = "DGerr.log"
    cmd = [
            f"./example -bs {bs} -bsm {bsm} -c {newchunk} -mp {mp} -mv {mv} -pa {pa} -p {custom_path_nisd}/ -pp {pp} -ps {ps} -seed {seed} -ss {endSeq} -t {t} -va {va} -vp {vp} -vs {vs} -vdev {vdev} -s3config {config_path}" ,
            ]

    print("cmd is: ", cmd)
    with open(stdoutdg_file, "a") as stdout, open(stderrdg_file, "a") as stderr:
        result = subprocess.run(cmd, shell=True, stdout=stdout, stderr=stderr)
        if result.returncode != 0:
            print(f"Error: The command '{cmd}' returned a non-zero exit code ({result.returncode}).")
            os.kill(GCProcess_Pid, signal.SIGKILL)
            sys.exit(1)  # Terminate the script with a non-zero exit code
    time.sleep(5)

def Get_seq_from_file(directory_path, prefix):
    if os.path.exists(directory_path):
        matching_files = [file for file in os.listdir(directory_path) if file.startswith(prefix)]
        if len(matching_files) > 1:
            return None
        else:
            parts = [file.split('.')[-1] for file in matching_files]
            last_value = parts[0] if parts else None
            print("End seq from ", prefix, " is: ", last_value)
            return last_value
    else:
        return None

while(1):
        newchunk = random.randint(1, num_chunks)
        should_break = False
        for  vdev in vdev_array:
            # CHeck the GC service is still running or not.
            # IF GCservice got terminated, print error about it and terminate the script.
            return_code = process.poll()
            if return_code is not None:
                print(f"Error: Process with PID has completed with return code {return_code}.")
                should_break = True
                break

            print("vdev value ", vdev)
            path = f"{custom_path_nisd}/{vdev}/DV/{newchunk}/dummy_generator.json"
            if os.path.exists(path):
                try:
                    json_data = json.loads(open(f'{custom_path_nisd}/{vdev}/DV/{newchunk}/dummy_generator.json'.format(custom_path_nisd)).read())
                    end_seq = json_data.get("SeqEnd", 0) + 1
                    print(f"Extracted and incremented 'SeqEnd' value: {end_seq}")
                except (json.JSONDecodeError, FileNotFoundError) as e:
                    print(f"Error extracting 'SeqEnd': {e}")
                generate_new_command(newchunk, seed, result_array, vdev, end_seq, GCProcess_Pid)
            else:
                end_seq = 0
                generate_new_command(newchunk, seed, result_array, vdev, end_seq, GCProcess_Pid)
            i = 0
            while(1):
                markerPath_nisd = f"{custom_path_nisd}/{vdev}/marker/{newchunk}/"
                nisdSeq = Get_seq_from_file(markerPath_nisd, 'nisdmrk')
                print(f"nisd end sequence is {nisdSeq} ")
                if nisdSeq == None:
                    continue
                markerPath_gc = f"{custom_path_gc}/{vdev}/marker/{newchunk}/"
                gcSeq = Get_seq_from_file(markerPath_gc, 'gcmrk')
                print(f"GC end sequence is {gcSeq} ")
                #if gcSeq == None:
                 #   continue
                if nisdSeq != gcSeq:
                    time.sleep(1)
                else:
                    cmd = [
                                 f"./dataValidator -d {custom_path_gc}/{vdev}/ -c {newchunk} -l {custom_path_nisd}/{vdev}/DV/{newchunk}/DV_{newchunk} -s {custom_path_nisd}/{vdev}/ ",
                          ]
#                    print("Running Datavalidation for chunk", newchunk, "and vdev", vdev, "and command", cmd)
                    result = subprocess.run(cmd, shell=True)
 #                   print("Return code of  DV  ", result.returncode)
                    if result.returncode != 0:
                        print(f"datavalidator failed with return code {result.returncode}  for chunk {newchunk} in vdev {vdev}")
                        time.sleep(1)
                        for i in range(10):
                            retry_result = subprocess.run(cmd, shell=True)
                            if retry_result.returncode == 0:
                                 break
                        if retry_result.returncode != 0:
                            should_break = True
                            break
                    if result.returncode == 0:
                        remove_old_folders(custom_path_gc,"In_",f"{vdev}_{newchunk}")
                     #   remove_old_folders(custom_path_gc,"Out_",f"{vdev}_{newchunk}")
                    break
                i+=1
                if i == 30:
                    print(f"i value is {i}")
                    break
            if should_break:
            #        os.kill(GCProcess_Pid, signal.SIGKILL)
                process.kill()
                break
        if should_break:
            break
        #Data+=1
