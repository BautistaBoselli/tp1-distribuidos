import os
import csv
from time import sleep
from random import choice

EXCLUDED_CONTAINERS = ['server', 'reviver-1', 'mapper-1', 'mapper-2']
NAME_IP_FILE = '../name_ip.csv'

def read_containers_from_name_ip_file():
    containers = []
    with open(NAME_IP_FILE, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if row[0] not in EXCLUDED_CONTAINERS:
                containers.append(row[0])
    return containers

def kill_container(container_name):
    res = os.system(f"docker stop {container_name} -s 9")
    if res != 0:
        print(f"Failed to kill {container_name}")
    else:
        print(f"Killed {container_name}")

def main():
    containers = read_containers_from_name_ip_file()
    print(f"Containers: {containers}")
    try:
        while True:
            sleep(4)
            container = choice(containers)
            print(f"Killing {container}\n")
            kill_container(container)
    except KeyboardInterrupt:
        print("\nExiting...")

if __name__ == "__main__":
    main()
