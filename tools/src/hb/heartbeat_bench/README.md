# Heartbeat mechanism benchmark

Tests the time from the moment a process crashes to the moment the kernel detects the crash.

## Build/Insert kernel module
```sh
cd module
make
insmod heartbeat
```

# Build userspace
First, create a shared memory region:
```sh
ipcmk --shmem 1024
```

Get the key of this memory region:
```sh
ipcs -m
```

And add this key in `userspace/src/CMakeLists.txt`, as the `SHM_MEM_KEY` definition. Then:
Compile the dependencies required by the userspace. The easiest way is:
```sh
/path/to/dory/root/build.py
```

Compile the userspace:
```sh
cd userspace
./conanfile.py --build-locally
```


## Loading
From the `heartbeat_benchmark` directory:
```sh
sudo insmod module/heartbeat.ko
```

You can check in `dmesg` that the module is loaded.

# Testing
From the `heartbeat_benchmark` directory:
```sh
userspace/build/bin/zeroshm # Clear the timestamps stored in the shm
userspace/build/bin/generatedata # Spawn an application that stores a timestamp in the shm and kills itself
```

Now, in dmesg you will have the timestamp when the heartbeat mechanism was invoked by the process. Copy that timestamp and run compute the difference:
```sh
userspace/build/bin/computetime <copied_timestamp>
```

The application and the kernel module report the core in which they run. Make sure that it is the same core.

## Unloading the heartbeat module
First, make sure that no application that uses the Failure Detector is running. Then: 
```
sudo module/unload.sh
```
