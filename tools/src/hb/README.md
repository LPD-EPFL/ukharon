# Failure Detection Module

These two kernel module are used to detect process failures in linux.

The fdmod module contains the function `heartbeat_send_multicast` that sends a multicast message using RDMA.
This function is invoked by the heartbeat module. This module requires a custom kernel.
It uses the `heartbeat_set_callback` of the custom kernel to register a callback that is invoked every time a process exits (due to failure or normal exit).
The process opts-in for this feature by using the PRCTL mechanism: The process notifies its process table that upon exit the set heartbeat callback should be invoked.

## Build

These two modules rely on RDMA. Stock RDMA drivers in the kernel are not regularly updated. Most kernels use the Mellanox OFED drivers that are compiled using DKMS.

To build the modules using the OFED drivers to the following (ofa_kernel v5.3 is described):
```sh
cd /var/lib/dkms/ofa_kernel/5.3
```

Uninstall the existing DKMS drivers. You can see all dkms drivers using `dkms status`:
```sh
sudo dkms uninstall -m ofa_kernel/5.3
```

Remove the build sources from this directory and leave only the `source` softlink.
```sh
sudo rm -r build 5.4.0-74-custom
```

Copy the modules to `source`:
```sh
sudo cp /path/to/fdmod source
sudo cp /path/to/heartbeat source
```

Make the modules build along with the rest of the OFED driver modules:
```sh
echo "obj-m += fdmod/fdmod.o" | sudo tee -a source/Makefile
echo "obj-m += heartbeat/heartbeat.o" | sudo tee -a source/Makefile
```

Edit the `source/ofed_scripts/dkms_ofed_post_build.sh` to keep these additional objects files when the OFED drivers are compiled.
To accomplish this, add something like this:
```sh
cp -r /var/lib/dkms/ofa_kernel/5.3/build /tmp
```
Right before the line `cd $build_dir`.

Build using DKMS:
```sh
sudo dkms build -m ofa_kernel/5.3
```

Install using DKMS:
```sh
sudo dkms install -m ofa_kernel/5.3
```

The two modules will be available in:
```sh
/tmp/build/fdmod/fdmod.ko
/tmp/build/heartbeat/heartbeat.ko
```
Copy them to a directory that persists across system reboots.

## Loading

```
sudo insmod /path/to/fdmod.ko [IPV6_GID=..] [DEVICE_NAME=..] [LID=..]
sudo insmod /path/to/heartbeat.ko
```

## Unloading
First, make sure that no application that uses the Failure Detector is running. Then call the two scripts provided in `fdmod/unload.sh` and `heartbeat/unload.sh`.
```
sudo heartbeat/unload.sh
sudo fdmod/unload.sh
```
