import os
import dispy
import time
import threading

def compute_sort_chunk(start, end, path, job_id):
    import socket
    import os
    
    # Simple sort implementation
    def bubble_sort(data):
        n = len(data)
        for i in range(n):
            for j in range(0, n - i - 1):
                if data[j] > data[j + 1]:
                    data[j], data[j + 1] = data[j + 1], data[j]
        return data

    nums = []
    with open(path, "r") as f:
        if start != 0:
            f.seek(start)
            while f.read(1) not in ["\n", ""]: pass

        remnant = ""
        while f.tell() <= end:
            chunk = f.read(min(4096, end - f.tell() + 1))
            if not chunk: break
            data = remnant + chunk
            lines = data.split('\n')
            remnant = lines.pop()
            for l in lines:
                if l.strip(): nums.append(int(l.strip()))
        
        if remnant:
            last = remnant + f.readline()
            if last.strip(): nums.append(int(last.strip()))

    sorted_data = bubble_sort(nums)
    temp_path = f"/mnt/nfsshare/data/worker_{job_id}.tmp"
    
    with open(temp_path, "w") as f:
        for n in sorted_data:
            f.write(f"{n}\n")
            
    return (socket.gethostname(), job_id, temp_path)

class ParallelManager:
    def __init__(self, output_path):
        self.output_path = output_path
        self.ready_files = []
        self.lock = threading.Lock()
        self.merge_count = 0
        self.completed_jobs = 0

    def merge_two_files(self, file1, file2):
        self.merge_count += 1
        merged_path = f"/mnt/nfsshare/data/merged_{self.merge_count}.tmp"
        
        with open(file1, 'r') as f1, open(file2, 'r') as f2, open(merged_path, 'w') as out:
            val1 = f1.readline().strip()
            val2 = f2.readline().strip()
            while val1 and val2:
                if int(val1) < int(val2):
                    out.write(val1 + "\n")
                    val1 = f1.readline().strip()
                else:
                    out.write(val2 + "\n")
                    val2 = f2.readline().strip()
            while val1:
                out.write(val1 + "\n")
                val1 = f1.readline().strip()
            while val2:
                out.write(val2 + "\n")
                val2 = f2.readline().strip()
        
        os.remove(file1)
        os.remove(file2)
        return merged_path

    def job_callback(self, job):
        _, _, result = job.result
        self.completed_jobs += 1
        with self.lock:
            self.ready_files.append(result)
            while len(self.ready_files) >= 2:
                f1 = self.ready_files.pop(0)
                f2 = self.ready_files.pop(0)
                new_temp = self.merge_two_files(f1, f2)
                self.ready_files.append(new_temp)

def run_distributed_sort():
    lines_limit = 1300000
    total_chunks = 96
    nodes = ['192.168.0.10', '192.168.0.20', '192.168.0.40']
    master_ip = '192.168.1.190' 
    source = "/mnt/usb/data2.set"
    temp_subset = "/mnt/nfsshare/processing_subset.set"
    
    os.makedirs("/mnt/nfsshare/data/", exist_ok=True)
    os.system(f"head -n {lines_limit} {source} > {temp_subset}")
    
    file_size = os.path.getsize(temp_subset)
    chunk_size = (file_size // total_chunks) + 1
    
    manager = ParallelManager("/mnt/nfsshare/final_output.txt")
    cluster = dispy.JobCluster(compute_sort_chunk, nodes=nodes, 
                               job_status=manager.job_callback, host=master_ip)
    
    start_time = time.time()

    for i in range(total_chunks):
        cluster.submit(i * chunk_size, (i + 1) * chunk_size, temp_subset, i)

    cluster.wait()
    
    while manager.completed_jobs < total_chunks:
        time.sleep(0.5)
    
    cluster.close()

    if len(manager.ready_files) == 1:
        os.rename(manager.ready_files[0], manager.output_path)

    print(f"Total time: {round(time.time() - start_time, 2)}s")

if __name__ == "__main__":
    run_distributed_sort()
