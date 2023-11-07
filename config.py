import configparser
import os

configer = configparser.ConfigParser()
current_dir = os.path.dirname(os.path.realpath(__file__))
configer.read(os.path.join(current_dir, 'config.ini'), encoding="utf-8")