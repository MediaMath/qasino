# Copyright (C) 2014 MediaMath, Inc. <http://www.mediamath.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
import fcntl
import struct
import uuid
import random

import qasino_table

class Identity(object):
    identity = 'unidentified'

    @staticmethod
    def get_ip_address_from_iface(ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(
                        fcntl.ioctl(
                              s.fileno(),
                              0x8915,  # SIOCGIFADDR
                              struct.pack('256s', ifname[:15])
                        )[20:24]
               )

    @staticmethod
    def get_ip_address_from_hostname():
        return socket.gethostbyname(socket.gethostname())

    @staticmethod
    def get_identity():
        
        # cached or pre-set?

        if Identity.identity == 'unidentified':

            # python newbie here..
            try:
                ip = Identity.get_ip_address_from_iface('eth0')
            except:
                try: 
                    ip = Identity.get_ip_address_from_iface('eth1')
                except:
                    try:
                        ip = Identity.get_ip_address_from_hostname()
                    except:
                        ip = None

            if ip == None or ip.startswith('127.') or ip.startswith('192.168.') or ip.startswith('10.'):
                # Use a mac address if we didn't get an ip address
                Identity.identity = str(uuid.getnode())
            else:
                Identity.identity = ip

        return Identity.identity

    @staticmethod
    def set_identity(_identity):
        Identity.identity = _identity



def pretty_print_table(outputter, table, max_widths=None, column_delim="  "):
    """
    Expects table to be:

    table = { "column_names" : [ "col1", "col2", "col3" ],
              "rows" : [ [ "row1cell1", "row1cell2", "row1cell3" ],
                         [ "row2cell1", "row2cell2", "row2cell3" ]
                       ]
            }

    Outputs:
         col1       col2       col3
    =========  =========  =========
    row1cell1  row1cell2  row1cell3
    row2cell1  row2cell2  row2cell3
    """

    if max_widths == None:

        max_widths = {}

        # Find the max widths of all the columns.

        # First check the column names.

        for column_index, column_name in enumerate(table["column_names"]):

            length = len(column_name)

            if not max_widths.has_key(str(column_index)) or max_widths[str(column_index)] < length:
                max_widths[str(column_index)] = length
    
        # Now the data.

        for row in table["rows"]:

            for column_index, cell in enumerate(row):

                length = len(unicode_safe_str(cell))

                if not max_widths.has_key(str(column_index)) or max_widths[str(column_index)] < length:
                    max_widths[str(column_index)] = length
                

    # Print the column names

    outputter.sendLine(
        column_delim.join( [ str(column_name.rjust(max_widths[str(index)]))
                             for index, column_name in enumerate(table["column_names"]) 
                           ] 
                         )
    )
    
    # Print ==== under column names

    outputter.sendLine(
        column_delim.join( [ "=" * max_widths[str(index)]
                             for index, column_name in enumerate(table["column_names"]) 
                           ] 
                         )
    )

    nr_rows = 0

    # Print the rows now.

    for row in table["rows"]:

        nr_rows += 1

        line = column_delim.join( [ unicode_safe_str(cell).rjust(max_widths[str(index)]) 
                                    for index, cell in enumerate(row) 
                                  ] 
                                )

        outputter.sendLine(line)

    # Now number of rows

    outputter.sendLine("%d rows returned" % nr_rows)

def unicode_safe_str(s):
    if s is None:    # this is an added "feature"
        return ''
    elif type(s) is unicode:
        # I can't get jinja in the http ui to encode as utf-8 instead of ascii (which barfs) so escape for now.
        #return s.encode('utf-8')
        return s.encode('unicode_escape')
    else:
        return str(s)


def random_string(start, stop):
    string = ""
    sizeof_string = random.randint(start, stop + 1)
    for x in range(sizeof_string):
        pick_a_char_index = random.randint(0, len(random_string.alphabet) - 1)
        string += random_string.alphabet[pick_a_char_index]
    return string

random_string.alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"



# Below included from:
# http://code.activestate.com/recipes/325204-passwd-file-compatible-1-md5-crypt/
# -------------------------------------------------------------------------------

# Based on FreeBSD src/lib/libcrypt/crypt.c 1.2
# http://www.freebsd.org/cgi/cvsweb.cgi/~checkout~/src/lib/libcrypt/crypt.c?rev=1.2&content-type=text/plain

# Original license:
# * "THE BEER-WARE LICENSE" (Revision 42):
# * <phk@login.dknet.dk> wrote this file.  As long as you retain this notice you
# * can do whatever you want with this stuff. If we meet some day, and you think
# * this stuff is worth it, you can buy me a beer in return.   Poul-Henning Kamp

# This port adds no further stipulations.  I forfeit any copyright interest.

import md5

def md5crypt(password, salt, magic='$1$'):
    # /* The password first, since that is what is most unknown */ /* Then our magic string */ /* Then the raw salt */
    m = md5.new()
    m.update(password + magic + salt)

    # /* Then just as many characters of the MD5(pw,salt,pw) */
    mixin = md5.md5(password + salt + password).digest()
    for i in range(0, len(password)):
        m.update(mixin[i % 16])

    # /* Then something really weird... */
    # Also really broken, as far as I can tell.  -m
    i = len(password)
    while i:
        if i & 1:
            m.update('\x00')
        else:
            m.update(password[0])
        i >>= 1

    final = m.digest()

    # /* and now, just to make sure things don't run too fast */
    for i in range(1000):
        m2 = md5.md5()
        if i & 1:
            m2.update(password)
        else:
            m2.update(final)

        if i % 3:
            m2.update(salt)

        if i % 7:
            m2.update(password)

        if i & 1:
            m2.update(final)
        else:
            m2.update(password)

        final = m2.digest()

    # This is the bit that uses to64() in the original code.

    itoa64 = './0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'

    rearranged = ''
    for a, b, c in ((0, 6, 12), (1, 7, 13), (2, 8, 14), (3, 9, 15), (4, 10, 5)):
        v = ord(final[a]) << 16 | ord(final[b]) << 8 | ord(final[c])
        for i in range(4):
            rearranged += itoa64[v & 0x3f]; v >>= 6

    v = ord(final[11])
    for i in range(2):
        rearranged += itoa64[v & 0x3f]; v >>= 6

    return magic + salt + '$' + rearranged

#if __name__ == '__main__':
#
#    def test(clear_password, the_hash):
#        magic, salt = the_hash[1:].split('$')[:2]
#        magic = '$' + magic + '$'
#        return md5crypt(clear_password, salt, magic) == the_hash
#
#    test_cases = (
#        (' ', '$1$yiiZbNIH$YiCsHZjcTkYd31wkgW8JF.'),
#        ('pass', '$1$YeNsbWdH$wvOF8JdqsoiLix754LTW90'),
#        ('____fifteen____', '$1$s9lUWACI$Kk1jtIVVdmT01p0z3b/hw1'),
#        ('____sixteen_____', '$1$dL3xbVZI$kkgqhCanLdxODGq14g/tW1'),
#        ('____seventeen____', '$1$NaH5na7J$j7y8Iss0hcRbu3kzoJs5V.'),
#        ('__________thirty-three___________', '$1$HO7Q6vzJ$yGwp2wbL5D7eOVzOmxpsy.'),
#        ('apache', '$apr1$J.w5a/..$IW9y6DR0oO/ADuhlMF5/X1')
#    )
#
#    for clearpw, hashpw in test_cases:
#        if test(clearpw, hashpw):
#            print '%s: pass' % clearpw
#        else:
#            print '%s: FAIL' % clearpw

# We'll use it like this for our cmp_pass function in qasino_server.py
def get_apache_md5(clear_password, the_hash):
    magic, salt = the_hash[1:].split('$')[:2]
    magic = '$' + magic + '$'
    return md5crypt(clear_password, salt, magic)

