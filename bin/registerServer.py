#!/usr/bin/python
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(sys.argv[0]) + "/../lib"))

import libPgSQL

from utils import named_exception_handler


@named_exception_handler('registerServer')
def main():
    dbCicada = libPgSQL.init_db()
    libPgSQL.registerServer(dbCicada)


if __name__ == "__main__":
    main()
