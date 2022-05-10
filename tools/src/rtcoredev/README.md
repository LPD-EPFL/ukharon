# RT Core loadable module

## Description
This kernel module exposes the rtcore patch created for the linux kernel.
It creates a character device named `/dev/rtcore` which a program can `open`, `close`, and `write`.

Currently, the driver has the following limitation:

Only one process can `open` the file. Other processes trying to `open` an already open file fail with `EBUSY`. Beware that forking a processes that has this file already openned can is undefined behaviour at this stage.


Finally, when writing to this file, the following sequences are supported:
1. Writing "0 0x0" halts the RT Core.
2. Writing "1 0x0" starts the RT Core without incrementing any counter.
3. Writint "1 0xdeadbeef" starts the RT Core and increments the counter at the physical address "0xdeadbeef". This address is treated as a 64-bit unsigned integer.

## Compiling
To compile the kernel driver successfully, the kernel has to be already patched. With this requirement in mind, to compile the driver simply
```sh
[rtcoredev] $ make
```
## Managing module
Load the module with
```sh
[rtcoredev] $ sudo insmod rtcore.ko
```

Unload the module with
```sh
[rtcoredev] $ echo "0x0 0" > /dev/rtcore
[rtcoredev] $ sudo rmmod rtcore
```
