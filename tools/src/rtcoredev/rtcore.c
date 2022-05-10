#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/module.h>

#include <linux/cdev.h>
#include <linux/device.h>

#include <asm/atomic.h>
#include <linux/ctype.h>
#include <linux/errno.h>

#include <asm/io.h>
#include <linux/fs.h>
#include <linux/sched.h>
#include <linux/uaccess.h>

#define PR_PREFIX "rtcore: "

int boot_rtcore(int cmd, unsigned long long addr);

static int rtcore_open(struct inode *inode, struct file *file);
static int rtcore_release(struct inode *inode, struct file *file);

// static long rtcore_ioctl(struct file *file, unsigned int cmd, unsigned long
// arg); static ssize_t rtcore_read(struct file *file, char __user *buf, size_t
// count, loff_t *offset);
static ssize_t rtcore_write(struct file *file, const char __user *buf,
                            size_t count, loff_t *offset);

static const struct file_operations rtcore_fops = {
    .owner = THIS_MODULE,
    .open = rtcore_open,
    .release = rtcore_release,
    // .unlocked_ioctl = rtcore_ioctl,
    // .read       = rtcore_read,
    .write = rtcore_write};

/* Device info */
static dev_t rtcore_dev = 0;
static struct class *rtcore_dev_class = NULL;
static struct cdev *rtcore_dev_data;
static atomic_t rtcore_dev_available = ATOMIC_INIT(1);

static int rtcore_uevent(struct device *dev, struct kobj_uevent_env *env) {
  add_uevent_var(env, "DEVMODE=%#o", 0666);
  return 0;
}

static int __init rtcore_init(void) {
  int result;

  // Request a new device number
  result = alloc_chrdev_region(&rtcore_dev, 0, 1, "rtcore");
  if (result != 0) {
    printk(KERN_ERR PR_PREFIX "failed to alloc chrdev region\n");
    goto fail_alloc_chrdev_region;
  }

  // Request a new device storage
  rtcore_dev_data = cdev_alloc();
  if (rtcore_dev_data == NULL) {
    result = -ENOMEM;
    printk(KERN_ERR PR_PREFIX "failed to alloc cdev\n");
    goto fail_alloc_cdev;
  }

  // Create the device class
  rtcore_dev_class = class_create(THIS_MODULE, "rtcore");
  if (rtcore_dev_class == NULL) {
    result = -EEXIST;
    printk(KERN_ERR PR_PREFIX "failed to create class\n");
    goto fail_create_class;
  }

  rtcore_dev_class->dev_uevent = rtcore_uevent;

  // Initialize device
  cdev_init(rtcore_dev_data, &rtcore_fops);
  rtcore_dev_data->owner = THIS_MODULE;

  result = cdev_add(rtcore_dev_data, rtcore_dev, 1);
  if (result < 0) {
    printk(KERN_ERR PR_PREFIX "failed to add cdev\n");
    goto fail_add_cdev;
  }

  // Create the device
  if (device_create(rtcore_dev_class, NULL, rtcore_dev, NULL, "rtcore") ==
      NULL) {
    result = -EINVAL;
    printk(KERN_ERR PR_PREFIX "failed to create device\n");
    goto fail_create_device;
  }

  return 0;

fail_create_device:
fail_add_cdev:
  class_unregister(rtcore_dev_class);
  class_destroy(rtcore_dev_class);
fail_create_class:
  cdev_del(rtcore_dev_data);
fail_alloc_cdev:
  unregister_chrdev_region(rtcore_dev, 1);
fail_alloc_chrdev_region:
  return result;
}

static void __exit rtcore_exit(void) {
  device_destroy(rtcore_dev_class, rtcore_dev);
  class_unregister(rtcore_dev_class);
  class_destroy(rtcore_dev_class);
  cdev_del(rtcore_dev_data);
  unregister_chrdev_region(rtcore_dev, 1);
}

static int rtcore_open(struct inode *inode, struct file *file) {
  if (!atomic_dec_and_test(&rtcore_dev_available)) {
    atomic_inc(&rtcore_dev_available);
    return -EBUSY;
  }

  return 0;
}

static int rtcore_release(struct inode *inode, struct file *file) {
  printk(KERN_INFO PR_PREFIX "involuntarily halting the RT core\n");
  boot_rtcore(0, 0ULL);
  atomic_inc(&rtcore_dev_available);
  return 0;
}

// static long mychardev_ioctl(struct file *file, unsigned int cmd, unsigned
// long arg)
// {
//     printk("MYCHARDEV: Device ioctl\n");
//     return 0;
// }

// static ssize_t mychardev_read(struct file *file, char __user *buf, size_t
// count, loff_t *offset)
// {
//     uint8_t *data = "Hello from the kernel world!\n";
//     size_t datalen = strlen(data);

//     printk("Reading device: %d\n",
//     MINOR(file->f_path.dentry->d_inode->i_rdev));

//     if (count > datalen) {
//         count = datalen;
//     }

//     if (copy_to_user(buf, data, count)) {
//         return -EFAULT;
//     }

//     return count;
// }

static ssize_t rtcore_write(struct file *file, const char __user *buf,
                            size_t count, loff_t *offset) {
  int cmd;
  unsigned long long addr;
  size_t cmd_idx;
  size_t addr_idx;
  int i = 0;

  char buffer[128];
  size_t not_copied;

  if (count > sizeof(buffer)) {
    return -EINVAL;
  }

  not_copied = copy_from_user(buffer, buf, count);

  if (not_copied != 0) {
    return -EINVAL;
  }

  while (i < count && isspace(buffer[i++]))
    ;
  cmd_idx = --i;

  while (i < count && !isspace(buffer[i++]))
    ;
  buffer[i - 1] = 0;

  while (i < count && isspace(buffer[i++]))
    ;
  addr_idx = --i;

  while (i < count && !isspace(buffer[i++]))
    ;
  buffer[--i] = 0;

  if (kstrtoint(buffer + cmd_idx, 0, &cmd)) return -EINVAL;

  if (cmd == 0) {
    printk(KERN_INFO PR_PREFIX "halting command sent RT core\n");
    boot_rtcore(cmd, 0ULL);
    goto end;
  } else if (cmd == 1) {
    if (kstrtoull(buffer + addr_idx, 0, &addr)) {
      return -EINVAL;
    }

    printk(KERN_INFO PR_PREFIX
           "starting RT core using physical address 0x%llx\n",
           addr);
    boot_rtcore(cmd, addr);
    goto end;
  } else {
    return -EINVAL;
  }

end:
  return count;
}

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Athanasios Xygkis <athanasios.xygkis@epfl.ch>");

module_init(rtcore_init);
module_exit(rtcore_exit);
