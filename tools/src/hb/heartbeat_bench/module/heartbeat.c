#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/module.h>

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>

#include <asm/atomic.h>

#include <linux/ctype.h>
#include <linux/errno.h>

#include <linux/heartbeat.h>

#include <asm/smp.h>

#include <linux/timekeeping.h>

MODULE_LICENSE("GPL");
MODULE_AUTHOR("DCL EPFL");
MODULE_DESCRIPTION("Demonstration module to use the heartbeat mechanism");
MODULE_VERSION("0.1");

#define PR_PREFIX "heartbeat: "

static atomic_t writes_left = ATOMIC_INIT(1);

/* Device info */
static dev_t dev = 0;
static struct class *dev_class = NULL;
static struct cdev *dev_data;

static int open(struct inode *inode, struct file *file);
static int release(struct inode *inode, struct file *file);
static ssize_t write(struct file *file, const char __user *buf, size_t count,
                     loff_t *offset);

static const struct file_operations fops = {
    .owner = THIS_MODULE, .open = open, .release = release, .write = write};

static int uevent(struct device *dev, struct kobj_uevent_env *env) {
  add_uevent_var(env, "DEVMODE=%#o", 0666);
  return 0;
}

static void callback(pid_t pid, int data) {
  s64 time;
  time = ktime_to_ns(ktime_get());
  printk(KERN_INFO PR_PREFIX "MONOTONIC_CLOCK ts = %lld ns\n", time);

  printk(KERN_INFO PR_PREFIX
         "Process %u invoked the callback by passing pointer %d as data\n",
         pid, data);
  printk(KERN_INFO PR_PREFIX "Callback executed on core %d\n",
         smp_processor_id());
}

static int initialization(void) {
  int result = 0;

  // Request a new device number
  result = alloc_chrdev_region(&dev, 0, 1, "heartbeat");
  if (result != 0) {
    printk(KERN_ERR PR_PREFIX "failed to alloc chrdev region\n");
    goto fail_alloc_chrdev_region;
  }

  // Request a new device storage
  dev_data = cdev_alloc();
  if (dev_data == NULL) {
    result = -ENOMEM;
    printk(KERN_ERR PR_PREFIX "failed to alloc cdev\n");
    goto fail_alloc_cdev;
  }

  // Create the device class
  dev_class = class_create(THIS_MODULE, "heartbeat");
  if (dev_class == NULL) {
    result = -EEXIST;
    printk(KERN_ERR PR_PREFIX "failed to create class\n");
    goto fail_create_class;
  }

  dev_class->dev_uevent = uevent;

  // Initialize device
  cdev_init(dev_data, &fops);
  dev_data->owner = THIS_MODULE;

  result = cdev_add(dev_data, dev, 1);
  if (result < 0) {
    printk(KERN_ERR PR_PREFIX "failed to add cdev\n");
    goto fail_add_cdev;
  }

  // Create the device
  if (device_create(dev_class, NULL, dev, NULL, "heartbeat") == NULL) {
    result = -EINVAL;
    printk(KERN_ERR PR_PREFIX "failed to create device\n");
    goto fail_create_device;
  }

  printk(KERN_INFO PR_PREFIX "Starting the heartbeat demo module\n");

  result = heartbeat_set_callback(callback);
  if (result == 1) {
    result = -EBUSY;
    printk(KERN_ERR PR_PREFIX
           "Could not register a callback when registered callers exist");
    goto fail_set_callback;
  }

  if (result == 2) {
    result = -EBUSY;
    printk(KERN_ERR PR_PREFIX "Could not register another callback");
    goto fail_set_callback;
  }

  printk(KERN_INFO PR_PREFIX "Callback registration succeeded\n");

  // Prevent unloading the module if a callers are registered
  if (!try_module_get(THIS_MODULE)) {
    result = -EBUSY;
    goto fail_inc_refcount;
  }

  return 0;

fail_inc_refcount:
fail_set_callback:
fail_create_device:
fail_add_cdev:
  class_unregister(dev_class);
  class_destroy(dev_class);
fail_create_class:
  cdev_del(dev_data);
fail_alloc_cdev:
  unregister_chrdev_region(dev, 1);
fail_alloc_chrdev_region:
  return result;
}

static int open(struct inode *inode, struct file *file) { return 0; }

static int release(struct inode *inode, struct file *file) { return 0; }

static ssize_t write(struct file *file, const char __user *buf, size_t count,
                     loff_t *offset) {
  int result;

  // Allow only one write to occur
  if (!atomic_dec_and_test(&writes_left)) {
    atomic_inc(&writes_left);
    return count;
  }

  result = heartbeat_unset_callback();
  if (result != 0) {
    printk(KERN_ERR PR_PREFIX "Could not unregister the callback");
    atomic_inc(&writes_left);
    return -EBUSY;
  }

  module_put(THIS_MODULE);

  return count;
}

static void termination(void) {
  device_destroy(dev_class, dev);
  class_unregister(dev_class);
  class_destroy(dev_class);
  cdev_del(dev_data);
  unregister_chrdev_region(dev, 1);

  printk(KERN_INFO PR_PREFIX "Exiting\n");
}

module_init(initialization);
module_exit(termination);
