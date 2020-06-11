import datetime
import logging
#%(asctime)s %(levelname)s %(pathname)s:%(lineno)d %(message)s
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)-8s %(pathname)s:%(lineno)d %(message)s')

log = logging.getLogger('tcpserver')