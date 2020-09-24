import argparse
import datetime
import glob
import hashlib
import io
import math
import os
import shutil
import subprocess
import sys
import threading
import time


parser = argparse.ArgumentParser(description='The programm does backup')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--hostnames', help='List of space separated hosts', nargs='+')
group.add_argument('--cleanup', help='Delete temp, bad md5 and files and in metas', action='store_true')
group.add_argument('--extract', help='Extract hash for meta into folder', nargs=2)
args = parser.parse_args()


# Универсальные вспомогательные функции и классы.
class O:
    ERASE_LINE = '\x1b[2K\r'

def o(s):sys.stdout.write(s);sys.stdout.flush()

def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()

def du(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size

def human(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def mtime(file_path): # возвращает время последней модификации файла
    stat = os.stat(file_path)
    return datetime.datetime.fromtimestamp(stat.st_mtime).strftime('%Y%m%d%H%M%S')

def fsize(file_path): # возвращает размер файл в байтах
    stat = os.stat(file_path)
    return stat.st_size


store_dir = os.path.dirname(os.path.realpath(__file__)) 
hash_dir = os.path.join(store_dir, 'hash')
meta_dir = os.path.join(store_dir, 'meta')
temp_dir = os.path.join(store_dir, 'temp')
o('Settings\n')
o(f'  root = {store_dir}\n')
o(f'  hash_dir = {hash_dir}\n')
o(f'  meta_dir = {meta_dir}\n')
o(f'  temp_dir = {temp_dir}\n')


if args.cleanup:
    o('Cleanup\n')
    o('  Check temp dir...\n')
    if os.path.exists(temp_dir):
        o(f'  Delete {os.path.abspath(temp_dir)}...')
        shutil.rmtree(temp_dir)

    o('  Check orphant files...\n')
    all_unique_md5 = set()
    o(f'    Build unique md5 list of found meta dirs:\n      ')
    for meta_dir in [x for x in glob.glob('meta*') if not os.path.islink(x)]:
        o(f'{meta_dir} ')
        for host in os.listdir(meta_dir):
            quick_txt = os.path.join(meta_dir, host, 'quick.txt')
            for line in open(quick_txt):
                chunks = line.split(' ')
                if len(chunks) > 2:
                    md5 = chunks[1]
                    all_unique_md5.add(md5)
    o(f'\n    Iterate over the hash dir and delete orphants if any...\n')
    deleted_count = 0
    for root, dirs, files in os.walk(hash_dir):
        for name in files:
            md5 = root[-2:]+name
            if (not md5 in all_unique_md5):
                file_remove = os.path.join(root, name)
                os.remove(file_remove)
                deleted_count+=1
                o(O.ERASE_LINE+f'      Deleted {deleted_count}, recent {path}, continue...')
    o(f'      Deleted {deleted_count}\n')

    # Делаем в последнюю очередь, т.к. самая долгая процедура.
    o('  Check files with bad md5...\n')
    deleted_count = 0
    for root, dirs, files in os.walk(hash_dir):
        for name in files:
            path = os.path.join(root,name)
            md5_recordered = root[-2:]+name
            md5_real = md5sum(path)
            o(O.ERASE_LINE+f'    Deleted {deleted_count}, calculate md5 for {path}...')
            if md5_recordered != md5_real:
                os.remove(path)
    o(O.ERASE_LINE+f'    Deleted {deleted_count}\n')
    sys.exit(0)

# Бэкапы сохраняются в "песочницу" - папку где лежит этот исполняемый скрипт dobackup.py.
# Убеждаемся и создаём если нет все дополнительные подпапки.
for d in [hash_dir,meta_dir,temp_dir]:
    if not os.path.exists(d):
        os.makedirs(d)
hostnames = args.hostnames
longest_hostname_length = len(sorted(hostnames, key=len)[-1])


# Запускаем на каждом хосте quick на папке /opt, для генерацияя файла с md5 всех файлов.
# И ждём окончания работы quick на всех хостах, где удалось запустить.
exclude_hostnames = []
for hostname in hostnames:
    o('Start quick @ %s'%hostname)
    subprocess.run("ssh root@%s killall -9 quick"%hostname, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    s = subprocess.run("scp quick root@%s:~/quick"%hostname, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if s.returncode:
        o(' Failed to scp binary!\n')
        exclude_hostnames.append(hostname)
        continue
    cmd = r'nohup sh -c "killall quick; chmod +x /root/quick; touch /tmp/quick_success && rm /tmp/quick_success && /root/quick /opt generate && touch /tmp/quick_success" >/dev/null 2>&1 &'
    s = subprocess.run("ssh -n root@%s '%s'"%(hostname, cmd), shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if s.returncode:
        o('Ssh connection failed!')
        exclude_hostnames.append(hostname)
    o('\n')
finished_hostnames = set()
processed_hostnames = set(hostnames) - set(exclude_hostnames)
elapsed = 0
while True:
    elapsed += 1
    for hostname in processed_hostnames:
        if not hostname in finished_hostnames:
            p = subprocess.run("ssh root@%s 'pgrep -x quick'"%hostname, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if p.returncode != 0:
                finished_hostnames.add(hostname)
    o('\x1b[2K\r'+'Wait for quick to finish ... %d/%d (%d) '%(len(finished_hostnames),len(processed_hostnames),elapsed))
    if len(finished_hostnames) == len(processed_hostnames):
        sys.stdout.write('\n')
        sys.stdout.flush()
        break
    time.sleep(1)

failed_hostnames = []
for hostname in processed_hostnames:
    print('%s ... '%hostname.ljust(longest_hostname_length),end='')

    hostname_temp_dir = os.path.join(temp_dir, hostname)
    os.makedirs(hostname_temp_dir, exist_ok=True)
    quick_filename = os.path.join(hostname_temp_dir, 'quick.txt')
    files_from_filename = os.path.join(hostname_temp_dir, 'files_from.txt')
    rsync_temp_dir = os.path.join(hostname_temp_dir, 'rsync_temp_dir')

    print('Scp quick.txt ... ', end='')
    p = subprocess.run(['scp','root@%s:/opt/quick.txt'%hostname,'%s'%quick_filename], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if p.returncode != 0:
        print('Failed!')
        failed_hostnames.append(hostname)
        continue

    # Сохраняем в download_lines[] список файлов, которых ещё нет на usb-диске.
    print('Parse ... ',end='')
    unique_md5s = set()
    download_lines = []
    download_size = 0
    quick_lines = [x.strip().split(' ', maxsplit=4) for x in open(quick_filename, errors='ignore').readlines() if x.startswith('md5 ')]
    quick_count = len(quick_lines)
    for (_,md5,_,size,path) in quick_lines:
        dest = os.path.join(hash_dir,md5[:2],md5[2:])
        size = int(size)
        if md5 in unique_md5s or os.path.exists(dest):
            continue
        unique_md5s.add(md5)
        download_lines.append(os.path.join('/opt/',path))
        download_size += size
    download_count = len(download_lines)
    
    # Сохраняем download_lines[] во временный файл, который отдаём в rsync и скачиваем в хэш-структуру:
    print('Rsync %s new files ---%%'%str(download_count).rjust(8, '0'), end='')
    open(files_from_filename,'w').write('\n'.join(download_lines))
    def downloader(cmd):
        subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
    cmd = ['rsync','-avz','--delete','--files-from=%s'%files_from_filename,'root@%s:/'%hostname,'%s'%rsync_temp_dir]
    t = threading.Thread(target=downloader, args=(cmd,))
    _, used_original, _ = shutil.disk_usage(store_dir)
    t.start()
    while t.is_alive():
        _, used_current, _ = shutil.disk_usage(store_dir)
        downloaded_approximate = used_current - used_original
        if download_size > 0:
            percentage = math.floor(downloaded_approximate*100/download_size)
            if percentage > 100:
                percentage = 99
        else:
            percentage = 100
        o('\b\b\b\b%s%%'%(str(percentage)).rjust(3))
        time.sleep(5)
    c = 0
    for remote_abs_path in download_lines:
        downloaded_file = rsync_temp_dir + remote_abs_path
        if os.path.exists(downloaded_file):
            c += 1
    if c == download_count:
        o('\b\b\b\b100%')
    
    if download_count > 0:
        o('    move to hash')
        skipped = 0
        quick_count_len = len(str(quick_count))
        for (_,md5,timestamp,size,rel) in quick_lines:
            hash_dest_dir = os.path.join(hash_dir,md5[:2])
            hash_dest_file = os.path.join(hash_dest_dir,md5[2:])
            remote_abs_path = os.path.join('/opt',rel)
            downloaded_file = rsync_temp_dir + remote_abs_path
            if not os.path.exists(hash_dest_file):
                if os.path.exists(downloaded_file):
                    if mtime(downloaded_file) != timestamp or str(fsize(downloaded_file)) != size:
                        skipped += 1
                    else:
                        os.makedirs(hash_dest_dir, exist_ok=True)
                        os.rename(downloaded_file, hash_dest_file)
        if skipped > 0:
            o(' (skipped %d)'%skipped)

    host_meta_dir = os.path.join(meta_dir, hostname)
    os.makedirs(host_meta_dir, exist_ok=True)
    meta_quickfile = os.path.join(host_meta_dir, 'quick.txt')
    shutil.copyfile(quick_filename, meta_quickfile)

    o('\n')


backup_size = du(hash_dir)
o('Hash size = %s\n'%(human(backup_size)))
