KERNEL      := /lib/modules/$(shell uname -r)/build
ARCH        := x86
C_FLAGS     := -Wall
KMOD_DIR    := $(shell pwd)
TARGET_PATH := /lib/modules/$(shell uname -r)/kernel/drivers/char

ccflags-y += $(C_FLAGS)

obj-m += rtcore.o

all:
	make -C $(KERNEL) M=$(KMOD_DIR) modules

install:
	cp rtcore.ko $(TARGET_PATH)
	depmod -a

uninstall:
	rm $(TARGET_PATH)/rtcore.ko
	depmod -a

clean:
	make -C $(KERNEL) M=$(KMOD_DIR) clean
