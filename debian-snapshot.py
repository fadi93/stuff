import urllib.request
import urllib.error
import http.client
import re
from pathlib import Path
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed
import json
from threading import Thread, Lock

# Thread-safe lock for shared resources
data_lock = Lock()

@retry(
    retry=(
        retry_if_exception_type(urllib.error.HTTPError) |
        retry_if_exception_type(http.client.HTTPException)
    ),
    wait=wait_fixed(5),
    stop=stop_after_attempt(100),
)
def get_timestamps(archive):
    """Fetches the timestamps for all available Debian snapshots for a given archive."""
    months = []
    with urllib.request.urlopen(f"https://snapshot.debian.org/archive/{archive}/") as f:
        for line in f:
            res = re.fullmatch(
                r'<a href="\./\?year=(?P<year>\d+)&amp;month=(?P<month>\d+)">\d+</a>\n',
                line.decode("utf-8"),
            )
            if res is None:
                continue
            months.append((int(res.group("year")), int(res.group("month"))))
    assert len(months) > 0
    return months

def fetch_timestamp_data(archive, year, month, existing_timestamps, outdir):
    """Fetches timestamp data for each Debian snapshot release."""
    url = f"https://snapshot.debian.org/archive/{archive}/?year={year}&month={month}"
    with urllib.request.urlopen(url) as f:
        for line in f:
            res = re.fullmatch(
                r"<a href=\"(\d{8}T\d{6}Z)/\">\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d</a><br />\n",
                line.decode("utf-8"),
            )
            if res is None:
                continue

            timestamp = res.group(1)
            if timestamp in existing_timestamps:
                print(f"Skipping already processed timestamp: {timestamp}")
                continue

            releases_list = fetch_with_redirect(
                f"https://snapshot.debian.org/archive/{archive}/{timestamp}/README", timestamp
            )
            if releases_list:
                with data_lock:  # Lock access to shared resources
                    # Add new data without overwriting existing entries
                    existing_data = load_existing_data(outdir / "debian.json")
                    if timestamp not in existing_data:
                        existing_data[timestamp] = releases_list
                    save_data_to_file(existing_data, outdir / "debian.json")
                    print(f"Saved data for timestamp: {timestamp}")

def fetch_with_redirect(url, timestamp, max_redirects=10):
    """Handles URL redirection and fetches the content."""
    redirects = 0
    while redirects < max_redirects:
        try:
            with urllib.request.urlopen(url) as response:
                return extract_debian_versions(response.read().decode('utf-8'), timestamp)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None  # Skip this URL
            if e.code in (301, 302, 303, 307, 308):
                url = e.headers.get('Location')
                redirects += 1
    return None

def extract_debian_versions(text, timestamp):
    """Extracts the Debian versions from the README content."""
    results = {}
    debian_pattern = re.compile(
        r"Debian\s+([\d.]+r?\d*)[^\n]*,\s+or\s+(\w+)\.\s+Access this release through\s+(\S+)",
        re.MULTILINE | re.IGNORECASE
    )

    for match in debian_pattern.finditer(text):
        version = match.group(1)
        version_name = match.group(2)
        if version_name not in results:
            results[version_name] = []
        results[version_name].append({"version": version, "timestamp": timestamp})
    return results

def load_existing_data(file_name):
    """Load existing data from a JSON file."""
    if Path(file_name).exists():
        with open(file_name, "r") as file:
            return json.load(file)
    return {}

def save_data_to_file(data, file_name):
    """Save the fetched data to a JSON file."""
    try:
        with open(file_name, "w") as file:
            json.dump(data, file, indent=4)
    except Exception as e:
        print(f"Error saving data to file: {e}")

def save_latest_timestamps(data, outdir):
    """
    Save the latest timestamp for each Debian version.

    Args:
        data (dict): The JSON data containing Debian releases.
        outdir (Path): The output directory where the stamps.json file will be saved.
    """
    latest_timestamps = {}
    
    # Iterate over all timestamps and their corresponding releases
    for timestamp, releases in data.items():
        for version_name, versions in releases.items():
            # Handle both lists and dictionaries for versions
            if isinstance(versions, list):
                for version_data in versions:
                    version = version_data["version"]
                    # Update with the latest timestamp for each version
                    if version not in latest_timestamps or timestamp > latest_timestamps[version]["timestamp"]:
                        latest_timestamps[version] = {
                            "version_name": version_name,
                            "timestamp": timestamp
                        }
            elif isinstance(versions, dict):
                version = versions["version"]
                # Update with the latest timestamp for each version
                if version not in latest_timestamps or timestamp > latest_timestamps[version]["timestamp"]:
                    latest_timestamps[version] = {
                        "version_name": version_name,
                        "timestamp": timestamp
                    }
    
    # Save the final result to the JSON file
    stamps_file = outdir / "stamps.json"
    save_data_to_file(latest_timestamps, stamps_file)
    print(f"Latest timestamps for each version saved to {stamps_file}")



def download_file(url, output_path):
    """Download a file from a URL to the specified output path."""
    if output_path.exists():
        print(f"File already exists: {output_path}, skipping download.")
        return
    try:
        print(f"Downloading: {url}")
        with urllib.request.urlopen(url) as response:
            with open(output_path, "wb") as out_file:
                out_file.write(response.read())
        print(f"File saved to: {output_path}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

def create_preseed_file(base_dir, stamps_file):
    """
    Create a preseed file for each version and save it in the respective directory.

    Args:
        base_dir (Path): The base directory where the JSON file is located.
        stamps_file (Path): Path to the stamps.json file containing version names and timestamps.
    """
    # Load the stamps.json data
    stamps_data = load_existing_data(stamps_file)

    # Define the preseed template
    preseed_template = """### Localization Settings
d-i debian-installer/language string en
d-i debian-installer/country string US
d-i debian-installer/locale string en_US

### Keyboard Settings
d-i console-keymaps-at/keymap select us
d-i keyboard-configuration/xkb-keymap select us

### Network Configuration
d-i netcfg/choose_interface select auto
d-i netcfg/get_hostname string unassigned-hostname
d-i netcfg/get_domain string unassigned-domain

### Firmware Loading
d-i hw-detect/load_firmware boolean true

### Mirror Configuration
d-i mirror/protocol string http
d-i mirror/http/hostname string snapshot.debian.org
d-i mirror/http/directory string /archive/debian/{timestamp}
d-i mirror/http/proxy string
d-i mirror/country string manual
d-i apt-setup/use_mirror boolean false
d-i apt-setup/services-select multiselect

### Static sources.list
d-i preseed/late_command string \\
echo "deb http://snapshot.debian.org/archive/debian/{timestamp} stable main" > /target/etc/apt/sources.list; \\
echo "deb http://snapshot.debian.org/archive/debian-security/{timestamp} stable/updates main" >> /target/etc/apt/sources.list; \\
export DEBIAN_FRONTEND=noninteractive; \\
apt-get update || true;

### Security Repository
d-i apt-setup/security_host string snapshot.debian.org
d-i apt-setup/security_path string /archive/debian-security/{timestamp}

##########################################################################
#####                       Partitioning                             #####
##########################################################################
d-i partman-auto/disk string /dev/hda /dev/sda /dev/vda /dev/cciss/c0d0
d-i partman-auto/method string regular
d-i partman-auto/expert_recipe string \\
      boot-root :: \\
              1000 1000 1024 ext3 \\
                      $primary{{ }} $bootable{{ }} \\
                      method{{ format }} format{{ }} \\
                      use_filesystem{{ }} filesystem{{ ext3 }} \\
                      mountpoint{{ /boot }} \\
              . \\
              16000 30128 32256 ext3 \\
                      $primary{{ }} label {{ }} \\
                      method{{ format }} format{{ }} \\
                      use_filesystem{{ }} filesystem{{ ext3 }} \\
                      mountpoint{{ / }} \\
              . \\
              2950 3 4096 linux-swap \\
                      label {{ SWAP }} \\
                      method{{ swap }} format{{ }} \\
              . \\
              1 1 1 ext3 method {{ keep }} .

d-i partman-lvm/device_remove_lvm boolean true
d-i partman-md/device_remove_md boolean true
d-i partman-crypto/confirm_nochanges boolean true
d-i partman-crypto/confirm_nooverwrite boolean true
d-i partman-lvm/confirm boolean true

d-i partman/confirm_write_new_label boolean true
d-i partman-partitioning/confirm_write_new_label boolean true
d-i partman/choose_partition select Finish partitioning and write changes to disk
d-i partman/confirm boolean true
d-i partman/confirm_nochanges boolean true
d-i partman/confirm_nooverwrite boolean true

##########################################################################
# NIS domain
d-i nis/domain string lab.mtl.com
nis nis/domain string lab.mtl.com

### Clock and time zone setup
d-i clock-setup/utc boolean false
d-i time/zone string Asia/Jerusalem
d-i clock-setup/ntp boolean true
d-i clock-setup/ntp-server string ntp

### Additional repositories
d-i apt-setup/backports boolean false
d-i apt-setup/contrib boolean false
d-i apt-setup/multiverse boolean false
d-i apt-setup/non-free boolean false
d-i apt-setup/proposed boolean false
d-i apt-setup/universe boolean false
d-i apt-setup/updates boolean false

### To create a normal user account.
d-i passwd/root-login boolean true
d-i passwd/username string herod
d-i passwd/root-password-crypted password $1$9rUl0.QT$aGM9nv26a6IlvyGPhl.Fu/
d-i passwd/make-user boolean false

### Grub installation
d-i grub-installer/bootdev string default
d-i grub-installer/with_other_os boolean true

### Package selection
tasksel tasksel/first multiselect standard
d-i pkgsel/include string openssh-server,xinit,nfs-kernel-server,debconf-utils,autofs,nis,ethtool,rsync,openipmi,ipmitool,mailutils,vim,lsb-release,mc,curl,lynx,strace,parted
d-i pkgsel/update-policy select none
d-i pkgsel/upgrade select none

### Finishing up the first stage install
d-i finish-install/reboot_in_progress note


###Postinstall
d-i preseed/late_command string \\
echo "***************** Executing Post install scripts *******************";\\
cp /var/log/partman /target/root/partman.log; cp /var/log/syslog /target/root/inst_syslog.log; mkdir /target/mnt/tmp;\\
apt-install apt-file; in-target /usr/bin/apt-file update; in-target apt-get -y remove mpt-status; \\
printf "domain lab.mtl.com server nis" >> /etc/yp.conf; \\
cp /etc/yp.conf /target/etc/yp.conf; \\
echo -e '3tango:3tango' | passwd root --stdin ;\\
printf "#!/bin/bash -x\\ncat /etc/resolv.conf >> /root/post2.log" >> /target/root/mount.script.sh ;\\
printf "\\nnslookup site-labfs01 >> /root/post2.log" >> /target/root/mount.script.sh ;\\
printf "\\ncat /etc/resolv.conf >> /root/post2.log" >> /target/root/mount.script.sh ;\\
printf "\\nnslookup site-labfs01 >> /root/post2.log" >> /target/root/mount.script.sh ;\\
chmod +x /target/root/mount.script.sh ;\\
printf "#!/bin/bash\\n/etc/init.d/rpcbind start\\n/etc/init.d/nfs-common start\\n/root/mount.script.sh >> /root/post1.log 2>&1\\nmount -o nolock site-labfs01:/vol/GL""IT /mnt/tmp >> /root/mount_post.log 2>&1\\n/bin/bash -x /mnt/tmp/autoinstall/postinstall_rs.sh 'multi-new nogrub' >> /root/postinstall.stdout  2>&1\\necho \"\" > /etc/rc.local" > /target/etc/rc.local ;\\
chmod +x /target/etc/rc.local 

d-i finish-install/reboot_in_progress note




"""

    # Iterate through the stamps data and create preseed files
    for version, version_info in stamps_data.items():
        version_name = version_info["version_name"]
        timestamp = version_info["timestamp"]

        print(f"Creating preseed file for version: {version} ({version_name}) with timestamp: {timestamp}")
        
        # Generate preseed content with the timestamp replaced
        preseed_content = preseed_template.format(timestamp=timestamp)
        
        # Save the preseed file in the appropriate directory
        version_dir = base_dir / version_name / version
        version_dir.mkdir(parents=True, exist_ok=True)
        preseed_file = version_dir / "preseed.cfg"

        # Write the preseed file
        if not preseed_file.exists():
            with open(preseed_file, "w") as file:
                file.write(preseed_content)
            print(f"Preseed file created: {preseed_file}")
        else:
            print(f"Preseed file already exists: {preseed_file}, skipping.")


def download_linux_and_initrd(base_dir, stamps_file):
    """
    Download the `linux` and `initrd.gz` files for the latest timestamp of each version.

    Args:
        base_dir (Path): The base directory where the JSON file is located.
        stamps_file (Path): Path to the stamps.json file containing version names and timestamps.
    """
    # Load the stamps.json data
    stamps_data = load_existing_data(stamps_file)

    for version, version_info in stamps_data.items():
        version_name = version_info["version_name"]
        timestamp = version_info["timestamp"]

        print(f"Processing version: {version} ({version_name}) with timestamp: {timestamp}")
        
        # Create a directory for this version name and timestamp
        version_dir = base_dir / version_name / version
        version_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate URLs for `linux` and `initrd.gz`
        base_url = f"https://snapshot.debian.org/archive/debian/{timestamp}/dists/{version_name}/main/installer-amd64/current/images/netboot/debian-installer/amd64"
        linux_url = f"{base_url}/linux"
        initrd_url = f"{base_url}/initrd.gz"

        # Download the files
        download_file(linux_url, version_dir / "linux")
        download_file(initrd_url, version_dir / "initrd.gz")


def process_month(archive, year, month, existing_timestamps, outdir):
    """Threaded function to process a specific month."""
    print(f"Processing year {year}, month {month}")
    fetch_timestamp_data(archive, year, month, existing_timestamps, outdir)

def main():
    outdir = Path("/snapshot/by-timestamp")
    outdir.mkdir(exist_ok=True)

    # Load existing data
    data_file = outdir / "debian.json"
    existing_data = load_existing_data(data_file)
    existing_timestamps = set(existing_data.keys())

    # Fetch timestamps and process them in threads
    timestamps = get_timestamps('debian')
    threads = []
    for year, month in timestamps[:]:  # Process all timestamps
        thread = Thread(target=process_month, args=('debian', year, month, existing_timestamps, outdir))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Save the latest timestamps for each version
    save_latest_timestamps(existing_data, outdir)

    # Download linux and initrd.gz files
    download_linux_and_initrd(outdir, outdir / "stamps.json")

     # Create preseed files for each version
    create_preseed_file(outdir, outdir / "stamps.json")
    print("All tasks completed.")


if __name__ == "__main__":
    main()
