import shlex
import subprocess


def fetch_credentials(file_name, key_name):
    command = shlex.split("cat $HOME/" + file_name + " | " + "grep " + key_name)
    print command
    res = subprocess.call(command)
    print res
    # return out.rsplit('=')[1]
