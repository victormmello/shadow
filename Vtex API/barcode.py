import sys

c = open('/dev/hidraw0', 'rb' )

code =c.read(12)

print(code)