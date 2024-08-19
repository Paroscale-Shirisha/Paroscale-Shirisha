import subprocess
import json
import sys
import time
import random
import uuid
import os
import signal
import concurrent.futures

custom_path_nisd = sys.argv[1]
custom_path_gc = sys.argv[2]
config_path = sys.argv[3]
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
vdev_array = []

j_values_datavalidation = []
j_values_dummygenerator = []
stdout_file = "Gc.log"
stderr_file = "GCerr.log"

# Start the GCservice here using subprocess.open it has to be run in back-ground.
command = f"./GCService -path {custom_path_gc} -s3config {config_path} -s3log {custom_path_gc}/GCs3log"
with open(stdout_file, "w") as stdout, open(stderr_file, "w") as stderr:
    process = subprocess.Popen(command, shell=True, stdout=stdout, stderr=stderr)
    GCProcess_Pid = process.pid

for vdev in range(num_vdev):
    uuid1 = uuid.uuid4()
    vdev_array.append(uuid1)

print("vdev array ", vdev_array)

# Function to generate new command
def generate_new_command(newchunk, result_array, vdev, endSeq, GCProcess_Pid):
    global seed, bsm, mp, mv, pa, ps, t, va, vp, vs

    if newchunk not in result_array:
        result_array.append(newchunk)

    bsm = random.randint(5, 32)
    mp = random.randint(10000, 500000)
    mv = random.randint(1024, 65536)
    pa = random.randint(10001, 655536)
    ps = random.randint(500, 50000)
    t = random.randint(0, 4)
    va = random.randint(1000000, 10000000)
    vp = random.randint(1000, 10000)
    vs = -1
    '''
    seed = random.randint(1, 10000)
    bsm = random.randint(5, 32)
    mp = random.randint(10, 50)
    mv = random.randint(1024, 65536)
    pa = random.randint(51, 65536)
    ps = random.randint(5, 50)
    t = random.randint(0, 4)
    va = random.randint(8096, 50000)
    vp = random.randint(10, 8095)
    vs = -1
    '''
    stdoutdg_file = f"DG_{newchunk}_{vdev}.log"
    stderrdg_file = f"DGerr_{newchunk}_{vdev}.log"
    cmd = [
        f"./example -bs {bs} -bsm {bsm} -c {newchunk} -mp {mp} -mv {mv} -pa {pa} -p {custom_path_nisd}/ -pp {pp} -ps {ps} -seed {seed} -ss {endSeq} -t {t} -va {va} -vp {vp} -vs {vs} -vdev {vdev} -s3config {config_path}",
    ]

    print("cmd is: ", cmd)
    with open(stdoutdg_file, "a") as stdout, open(stderrdg_file, "a") as stderr:
        result = subprocess.run(cmd, shell=True, stdout=stdout, stderr=stderr)
        if result.returncode != 0:
            print(f"Error: The command '{cmd}' returned a non-zero exit code ({result.returncode}).")
            os.kill(GCProcess_Pid, signal.SIGKILL)
            sys.exit(1)  # Terminate the script with a non-zero exit code
    time.sleep(5)

# Function to get sequence from file
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

# Worker function for processing chunks
def worker(chunk_num, vdev):
    while True:
        should_break = False
        print(f"Checking NISD and GC markers for chunk {chunk_num} in vdev {vdev}")

        # Check if the GC service is still running or not.
        # If GC service got terminated, print error about it and terminate the script.
        return_code = process.poll()
        if return_code is not None:
            print(f"Error: GC Process with PID {GCProcess_Pid} has failed with return code {return_code}.")
            break

        path = f"{custom_path_nisd}/{vdev}/DV/{chunk_num}/dummy_generator.json"
        if os.path.exists(path):
            try:
                json_data = json.loads(open(f'{custom_path_nisd}/{vdev}/DV/{chunk_num}/dummy_generator.json').read())
                end_seq = json_data.get("SeqEnd", 0) + 1
                print(f"Extracted and incremented 'SeqEnd' value: {end_seq}")
            except (json.JSONDecodeError, FileNotFoundError) as e:
                print(f"Error extracting 'SeqEnd': {e}")
            generate_new_command(chunk_num, result_array, vdev, end_seq, GCProcess_Pid)
        else:
            end_seq = 0
            generate_new_command(chunk_num, result_array, vdev, end_seq, GCProcess_Pid)

        while True:
            markerPath_nisd = f"{custom_path_nisd}/{vdev}/marker/{chunk_num}/"
            markerPath_gc = f"{custom_path_gc}/{vdev}/marker/{chunk_num}/"

            nisdSeq = Get_seq_from_file(markerPath_nisd, 'nisdmrk')
            gcSeq = Get_seq_from_file(markerPath_gc, 'gcmrk')

            if nisdSeq != gcSeq:
                time.sleep(1)
                continue
            else:
                cmd = [
                    f"./dataValidator -d {custom_path_gc}/{vdev}/ -c {chunk_num} -l {custom_path_nisd}/{vdev}/DV/{chunk_num}/DV_{chunk_num} -s {custom_path_nisd}/{vdev}/ ",
                ]
                result = subprocess.run(cmd, shell=True)
                if result.returncode != 0:
                    print(f"datavalidator failed with return code {result.returncode}  for chunk {chunk_num} in vdev {vdev}")
                    time.sleep(1)
                    for i in range(10):
                        retry_result = subprocess.run(cmd, shell=True)
                        if retry_result.returncode == 0:
                            break
                    if retry_result.returncode != 0:
                        should_break = True
                        break
                    #should_break = True
                    #break
                break

        if should_break:
            os.kill(GCProcess_Pid, signal.SIGKILL)
            break

# Main loop to continuously generate worker threads
with concurrent.futures.ThreadPoolExecutor(max_workers=num_chunks * num_vdev) as executor:
    for vdev in vdev_array:
        for chunk_num in range(1, num_chunks + 1):
            executor.submit(worker, chunk_num, vdev)

