from gcn_kafka import Consumer
import json
from base64 import b64decode
from io import BytesIO
from astropy.table import Table
import astropy_healpix as ah
import numpy as np
import requests


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
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={"1088194493"}&text={message}"
        print(requests.get(url).json())

TOKEN = "7587835515:AAGANBs7N9DiLO-hD4slIqEwrBF-v3MOtMs"
chat_ids = ["1088194493", "1131147560", "1664623255", "1031475863", "5154438801", 
            "265274642", "90028287", "981678835"]
consumer = Consumer(client_id='5g8a3hoef77ledgt7kl5ae2ef3',
                    client_secret='5n5knm0cf1d4u8pfott6iav2b6j593s29au303vrtmljtggkanl')
consumer.subscribe(['igwn.gwalert'])

while True:
    for message in consumer.consume(timeout=1):
        try:
            parse_notice(message.value())
        except:
            continue





