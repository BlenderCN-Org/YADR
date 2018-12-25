import sys
import subprocess

args = sys.argv[1:]

f = open(args[0])
for l in f:
	s = l.rstrip()
	subprocess.call(["ls", s])
