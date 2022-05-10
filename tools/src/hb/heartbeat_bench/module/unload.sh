#!/bin/sh

# Change the callback in the heartbeat module
# For this to succeed, no process must be using the heartbeat mechanism
echo '1' | sudo tee /dev/heartbeat

sudo rmmod heartbeat
