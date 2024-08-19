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
bucket_name = sys.argv[6]
Sleeptime = int(sys.argv[7])
loglevel = int(sys.argv[8])
promport = sys.argv[9]
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
chunk_array = []

j_values_datavalidation = []
j_values_dummygenerator = []
stdout_file = "Gc.log"
stderr_file = "GCerr.log"

# Define process globally
process = None

# Start the GCservice here using subprocess.open it has to be run in back-ground.
#command = f"./GCService -path {custom_path_gc} -s3config {config_path} -s3log {custom_path_gc}/GCs3log -memprofile=memprof"
command = f"./GCService -path {custom_path_gc} -s3config {config_path} -s3log {custom_path_gc}/GCs3log -t {Sleeptime} -l {loglevel} -p {promport} -b={bucket_name}"
with open(stdout_file, "w") as stdout, open(stderr_file, "w") as stderr:
    process = subprocess.Popen(command, shell=True, stdout=stdout, stderr=stderr)
    GCProcess_Pid = process.pid

if process is None:
    print("Error: Failed to start GCService process.")
    sys.exit(1)

for vdev in range(num_vdev):
    uuid1 = uuid.uuid4()
    vdev_array.append(uuid1)
print("vdev array " , vdev_array)

for chunk in range(num_chunks):
    chunk_array.append(chunk)
print("chunk array " , chunk_array)

def GenerateAndExecuteCommand(newchunk, seed, result_array, vdev, endSeq, GCProcess_Pid):
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
    vp = random.randint(10000, 100000)
    vs = -1
    stdoutdg_file = "DG.log"
    stderrdg_file = "DGerr.log"
    cmd = [
            f"./dummyData -bs {bs} -bsm {bsm} -c {newchunk} -mp {mp} -mv {mv} -pa {pa} -p {custom_path_nisd}/ -pp {pp} -ps {ps} -seed {seed} -ss {endSeq} -t {t} -va {va} -vp {vp} -vs {vs} -vdev {vdev} -s3config {config_path} -b={bucket_name}",
            ]

    print("cmd is: ", cmd)
    with open(stdoutdg_file, "a") as stdout, open(stderrdg_file, "a") as stderr:
        result = subprocess.run(cmd, shell=True, stdout=stdout, stderr=stderr)
        if result.returncode != 0:
            print(f"Error: The command '{cmd}' returned a non-zero exit code ({result.returncode}).")
            os.kill(GCProcess_Pid, signal.SIGKILL)
            sys.exit(1)  # Terminate the script with a non-zero exit code
    time.sleep(5)

def Get_seq_from_file(directory, second_element, last_element):
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_name = os.path.basename(file_path)
            file_parts = file_name.split('.')
            if len(file_parts) >= 3 and file_parts[1] == second_element and file_parts[-1] == last_element:
                file_list.append(file_parts[2])
                if file_list:
                    return file_parts[2]
    if not file_list:
        return None
    else:
        return file_parts[2]

while(1):
        for newchunk in chunk_array:
            print(f"Newchunk value is: {newchunk} ")
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
                path = f"{custom_path_nisd}/{vdev}/{newchunk}/DV/dummy_generator.json"
                if os.path.exists(path):
                    try:
                        json_data = json.loads(open(f'{custom_path_nisd}/{vdev}/{newchunk}/DV/dummy_generator.json'.format(custom_path_nisd)).read())
                        end_seq = json_data.get("SeqEnd", 0) + 1
                        print(f"Extracted and incremented 'SeqEnd' value: {end_seq}")
                    except (json.JSONDecodeError, FileNotFoundError) as e:
                        print(f"Error extracting 'SeqEnd': {e}")
                    GenerateAndExecuteCommand(newchunk, seed, result_array, vdev, end_seq, GCProcess_Pid)
                else:
                    end_seq = 0
                    GenerateAndExecuteCommand(newchunk, seed, result_array, vdev, end_seq, GCProcess_Pid)
                i=0
                while(1):
                    markerPath_nisd = f"{custom_path_nisd}/{vdev}/m/"
                    nisdSeq = Get_seq_from_file(markerPath_nisd, f'{newchunk}','nisd')
                    print(f"nisd end sequence is {nisdSeq} ")
                    markerPath_gc = f"{custom_path_gc}/{vdev}/m/"
                    gcSeq = Get_seq_from_file(markerPath_gc, f'{newchunk}','gc')
                    print(f"GC end sequence is {gcSeq} ")
                    if nisdSeq != gcSeq or nisdSeq is None or gcSeq is None:
                        time.sleep(1)
                    else:
                        cmd = [
                                     f"./dataValidator -d {custom_path_gc}/{vdev}/ -c {newchunk} -l {custom_path_nisd}/{vdev}/{newchunk}/DV/DV_{newchunk} -s {custom_path_nisd}/{vdev}/ ",
                              ]
                        result = subprocess.run(cmd, shell=True)
                        if result.returncode != 0:
                            print(f"datavalidator failed with return code {result.returncode}  for chunk {newchunk} in vdev {vdev}")
                            time.sleep(1)
                            for i in range(10):
                                retry_result = subprocess.run(cmd, shell=True)
                                if retry_result.returncode == 0:
                                     break
                            if retry_result.returncode != 0:
                                should_break = False
                                break
                        break
                    i+=1
                    if i == 30:
                        print(f"i value is {i}")
                        break
                if should_break:
                    process.kill()
                    break
            if should_break:
                break
        if should_break:
                break
