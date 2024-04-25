#!/usr/bin/env python
import subprocess
import sys
import os


def main():
    # Adjust the path to where your actual shell script is located
    script_dir = os.path.join(os.path.dirname(__file__))
    script_path = os.path.join(script_dir, 'medusa-wrapper.sh')
    args = sys.argv[1:]
    command = [script_path] + args
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if stderr:
        print(stderr, file=sys.stderr)
    if stdout:
        print(stdout, file=sys.stdout)
    rc = p.returncode
    sys.exit(rc)


if __name__ == "__main__":
    main()
