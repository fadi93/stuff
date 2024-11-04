import requests,re,threading,os,shutil,subprocess,time,stat
from bs4 import BeautifulSoup
from queue import Queue



downloadTotal = 0
downloadDone = 0
threadLock = threading.Lock()


class Downloader(threading.Thread):
    def __init__(self,threadNum, queue) -> None:
        threading.Thread.__init__(self)
        self.threadNum = threadNum
        self.kill_received = False
        self.queue = queue

   

    def run(self):
        global downloadDone

        while not self.kill_received and not self.queue.empty():
            parms = self.queue.get()
            url, dest = parms
            self.download_file(url, dest,  (self.threadNum % 7))
            with threadLock:
                downloadDone += 1
                print('\nDownload of {0} completed, {1} of {2}\n'.format(url, downloadDone, downloadTotal))

            self.queue.task_done()
        print('\nThread #{0} exiting\n'.format(self.threadNum),(self.threadNum % 7))

    def download_file(self,url_to_download,dest,thread_num):
        """Download the file from the URL."""
        file_name = os.path.join(dest, url_to_download.split("/")[-1])
        try:
            response = requests.get(url_to_download, stream=True)
            response.raise_for_status()  # Check for HTTP errors
            if not os.path.exists(file_name):
                version_dir = file_name.split('-amd64')[0]
                os.makedirs(version_dir, exist_ok=True)
            # Write the file in chunks to avoid memory overload
                full_path = version_dir + '/'+file_name.split('/')[-1]
                print(full_path)
                if os.path.exists(full_path):
                    return
            with open(full_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Downloaded: {file_name}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {url_to_download}: {e}")


   



def get_versions(url):
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()
        # Parse the HTML content with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the table by ID
        table = soup.find('table', id='indexlist')

        # Initialize an empty list to store Debian versions
        debian_versions = []

        # Find all rows in the table with class 'even' or 'odd' to extract version names
        for row in table.find_all('tr', class_=['even', 'odd']):
            # Find the link within the column that contains the version name
            version_link = row.find('td', class_='indexcolname').find('a')
            if version_link:
                version_name = version_link.text.strip().strip('/')
                if re.match(r'^(1[0-9]|[2-9]\d)\.\d+\.\d+$', version_name):
                    debian_versions.append(f'https://get.debian.org/images/archive/{version_name}/amd64/iso-cd/debian-'+version_name+'-amd64-netinst.iso')

        # Print or use the list of Debian versions
        print(debian_versions)
        return debian_versions

def has_live_threads(threads):
        return True in [t.is_alive() for t in threads]



##############################EXTRACT AND MODIFY INITRD ##############################

def process_initrd(root_dir):
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".iso"):
                iso_path = os.path.join(subdir, file)
                extract_dir = os.path.join(subdir, "extracted")
                try:


                    cdrom = os.path.join(extract_dir, 'install.amd/cdrom')
                    # Define the source and destination directories
                    src_dirs = [
                        os.path.join(extract_dir, '.disk'),
                        os.path.join(extract_dir, 'pool'),
                        os.path.join(extract_dir, 'dists'),
                        ]

                    # Create the destination directory if it doesn't exist
                    os.makedirs(cdrom, exist_ok=True)

                    # Copy each directory from src_dirs to dest_dir
                    for src in src_dirs:
                        shutil.copytree(src, os.path.join(cdrom, os.path.basename(src)), dirs_exist_ok=True)

                    print("Directories copied successfully.")

                    
                    # Add `sed -i '2s/-e/-x/'` command to specified file
                    setup_script = os.path.join(extract_dir, "install.amd/usr/lib/base-installer.d/20console-setup")
                    subprocess.run(["sed", "-i", "2s/-e/-x/", setup_script], check=True)

                    # Execute ./initrd/usr/lib/base-installer.d/99copy-cdrom
                    copy_cdrom_script = os.path.join(extract_dir, "install.amd/usr/lib/base-installer.d/99copy-cdrom")
                    with open(copy_cdrom_script, "a") as script_file:
                        script_file.write(
                            "#!/bin/sh\n"
                            "set -e\n"
                            ". /usr/share/debconf/confmodule\n\n"
                            "cp -r /cdrom  /target/media/cdrom\n"
                            "sed -i '2s/-e/-x/' /usr/lib/apt-setup/generators/50mirror\n"
                            "sed -i  '124s/use_mirror=false/use_mirror=true/g' /usr/lib/apt-setup/generators/50mirror"
                        )
                    os.chmod(copy_cdrom_script, os.stat(copy_cdrom_script).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                    # Append custom script lines to ./initrd/usr/lib/finish-install.d/11delete-cdrom
                    finish_install_script = os.path.join(extract_dir, "install.amd/usr/lib/finish-install.d/11delete-cdrom")
                    with open(finish_install_script, "a") as script_file:
                        script_file.write(
                            "#!/bin/sh\n"
                            "set -e\n"
                            ". /usr/share/debconf/confmodule\n\n"
                            "rm -rf /target/media/cdrom/*\n"
                        )

                    os.chmod(finish_install_script, os.stat(finish_install_script).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


                    # Run find command and recreate initrd file
                    initrd_path = os.path.join(extract_dir, "install.amd/initrd-iso.gz")
                    os.chdir(os.path.join(extract_dir,'install.amd'))

                    result = subprocess.run(
                    "find . | cpio -o -H newc | gzip > initrd-iso.gz",
                    shell=True,
                    stderr=subprocess.PIPE)


                   

                    # Copy the new initrd to /var/lib/tftpboot/
                    os.makedirs(f"/var/lib/tftpboot/debian_processed/{iso_path.split('/')[-2]}",exist_ok=True)
                    shutil.copy(initrd_path, f"/var/lib/tftpboot/debian_processed/{iso_path.split('/')[-2]}")
                    shutil.copy(subdir+'/extracted/install.amd/vmlinuz', f"/var/lib/tftpboot/debian_processed/{iso_path.split('/')[-2]}")

                    
                    print(f"Processed and updated initrd for {iso_path}")

                except Exception as e:
                    print(f"Error processing {iso_path}: {e}")
                finally:
                    # Cleanup extracted directory
                    #shutil.rmtree(extract_dir, ignore_errors=True)
                    pass

def process_debian_isos(root_dir):
    # Iterate over each subdirectory in the root directory
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".iso"):
                iso_path = os.path.join(subdir, file)
                extract_dir = os.path.join(subdir, "extracted")
                
                # Step 1: Extract ISO using 7z
                print(f"Extracting {iso_path} to {extract_dir}...")
                os.makedirs(extract_dir, exist_ok=True)
                if not len(os.listdir(extract_dir)) > 0 : 

                    # Ensure the extraction completes before moving on
                    with subprocess.Popen(["7z", "x", iso_path, f"-o{extract_dir}"],
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
                            stdout, stderr = proc.communicate()
                            if proc.returncode != 0:
                                print(f"Error extracting ISO: {stderr.decode().strip()}")
                                return
                            else:
                                print(f"Extraction successful:\n{stdout.decode().strip()}")
                    print(f"{extract_dir}/install.amd/initrd.gz")

                    # Change to the directory where initrd.gz is located
                    os.chdir(os.path.dirname(f"{extract_dir}/install.amd/"))
                    with subprocess.Popen(f"zcat initrd.gz | cpio -i ", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
                        stdout, stderr = proc.communicate()
                        
                        # Check for errors
                        if proc.returncode != 0:
                            print(f"Error extracting initrd: {stderr.decode().strip()}")
                            return
                        else:
                            print(f"Extraction successful:\n{stdout.decode().strip()}")
                

################################# END OF INITRD SECTION #########################3


def main():
    threads = []
    q = Queue(maxsize=0)

    versions = get_versions('https://get.debian.org/images/archive/')
    for dlParm in versions[:]:
        dl = dlParm,'/images/debian-versions'
        q.put(dl)

  

    for i in range(20):
        worker = Downloader(i, q)
        worker.start()
        threads.append(worker)

    while has_live_threads(threads):
        try:
            [t.join(1) for t in threads if t is not None and t.is_alive()]
        except KeyboardInterrupt:
            print('\nSending kill to threads, you will need to wait for the remaining downloads to finish (the impatient will need to kill from console)....\n')
            for t in threads:
                t.kill_received = True

    print('All items downloaded\n\n')

    # Specify the directory where the ISO files are located
    iso_directory = "/images/debian-versions"
    process_debian_isos(iso_directory)
    process_initrd(iso_directory)

main()