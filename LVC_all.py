from gcn_kafka import Consumer
import json
from base64 import b64decode
from io import BytesIO
from astropy.table import Table
import astropy_healpix as ah
import numpy as np
import requests
import os
from dotenv import load_dotenv


dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
def parse_notice(record):
    record = json.loads(record)
    if record['superevent_id'][0] != 'S':                 
        return
    if record['alert_type'] == 'RETRACTION':              
        print(record['superevent_id'], 'was retracted')
        return
    if record['event']['group'] != 'CBC':                 
        return
    skymap_str = record.get('event', {}).pop('skymap')    
    if skymap_str:
        skymap_bytes = b64decode(skymap_str) 
        skymap = Table.read(BytesIO(skymap_bytes))
        level, ipix = ah.uniq_to_level_ipix(
            skymap[np.argmax(skymap['PROBDENSITY'])]['UNIQ']
        )
        ra, dec = ah.healpix_to_lonlat(ipix, ah.level_to_nside(level),
                                       order='nested')
        print(f'Most probable sky location (RA, Dec) = ({ra.deg}, {dec.deg})')
        print(f'Distance = {skymap.meta["DISTMEAN"]} +/- {skymap.meta["DISTSTD"]}')
    BNS = float(record['event']['classification']['BNS'])
    NSBH = float(record['event']['classification']['NSBH'])
    print(BNS)
    print(NSBH)
    if (BNS + NSBH) > 0.3:                                
        for chat_id in chat_ids:
            message = f"BNS {BNS} NSBH {NSBH}   " + record["urls"]["gracedb"]
            url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
            print(requests.get(url).json())
    else:                                               
        message = f"BNS {BNS} NSBH {NSBH}   " + record["urls"]["gracedb"]
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={os.getenv('chat_id')}&text={message}"
        print(requests.get(url).json())

TOKEN = os.getenv('TOKEN')
chat_ids = os.getenv('chat_ids')
consumer = Consumer(client_id=os.getenv('client_id'),
                    client_secret=os.getenv('client_secret'))
consumer.subscribe(['igwn.gwalert'])

while True:
    for message in consumer.consume(timeout=1):
        try:
            parse_notice(message.value())
        except:
            continue





