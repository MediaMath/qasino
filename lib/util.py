
import socket
import fcntl
import struct
import uuid

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

                length = len(str(cell))

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

        line = column_delim.join( [ str(cell).rjust(max_widths[str(index)]) 
                                    for index, cell in enumerate(row) 
                                  ] 
                                )

        outputter.sendLine(line)

    # Now number of rows

    outputter.sendLine("%d rows returned" % nr_rows)
