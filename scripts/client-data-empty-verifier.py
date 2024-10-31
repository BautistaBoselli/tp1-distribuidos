import os
import time

def main():
    try :
        while True:
            time.sleep(1)
            os.system('clear')
            directories = os.listdir('../../database')
            counter = 0
            for directory in sorted(directories):
                data = os.listdir(f'../../database/{directory}')
                if len(data) > 0:
                    print(f"Directory {directory} contains: {len(data)} items")
                    counter += 1
            if counter == 0:
                print("All databases are empty, no data to display")
                continue
    
    except KeyboardInterrupt:
        print("Exiting...")
        exit(0)

if __name__ == '__main__':
    main()