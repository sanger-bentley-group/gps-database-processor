# This module saves the global variables shared by all modules.


import bin.colorlog


def init():
    global LOG
    LOG = bin.colorlog.get_log()