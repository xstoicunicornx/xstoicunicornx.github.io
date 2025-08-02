import re
from datetime import datetime
import pandas as pd

pd.set_option('display.max_rows', None)
unhanded_lines = []

def parse_datetime(datetime_string):
    if '.' not in datetime_string:
        datetime_string = datetime_string.replace('Z', '.0Z')
    return datetime.strptime(datetime_string, '%Y-%m-%dT%H:%M:%S.%fZ')

with open('debug.log') as f:
    # cache will hold block until update tip message is read
    # cache entries should be structured as: 
    #   {
    #       'block': <blockhash>,
    #       'height': <blockheight>,
    #       'initialized': <timestamp of cmpctblock initialized>,
    #       'reconstructed': <timestamp of cmpctblock reconstructed>,
    #       'tip_updated': <timestamp of update tip>,
    #       'prefilled_txs': <number of prefilled txs>,
    #       'mempool_txs': <number of txs taken from mempool>,
    #       'extrapool_txs': <number of txs taken from extra pool>,
    #       'requested_txs': <number of txs requested from peer>
    #   }
    #   
    #   all fields are optional except for block
    cache = []
    for line in f:
        entry = {}

        if re.search(r'^.*\[cmpctblock\] Initialized PartiallyDownloadedBlock.*$', line):
            words = line.split(' ')
            entry['initialized'] = parse_datetime(words[0])
            entry['block'] = words[6]
        elif re.search('Saw new cmpctblock header', line):
            words = line.split(' ')
            entry['initialized'] = parse_datetime(words[0])
            entry['block'] = words[5].replace('hash=', '')
        elif re.search(r'^.*\[cmpctblock\] Successfully reconstructed.*$', line):

            words = line.split(' ')
            entry['reconstructed'] = parse_datetime(words[0])
            entry['block'] = words[5]
            entry['prefilled_txs'] = int(words[7])
            entry['mempool_txs'] = int(words[10])
            entry['extrapool_txs'] = int(words[17])
            entry['requested_txs'] = int(words[22])
        # this message seems to log individual txs that were needed so ignore 
        elif re.search(r'^.*\[cmpctblock\] Reconstructed.*$', line):
            # words = line.split(' ')
            # entry['reconstructed'] = datetime.strptime(words[0], '%Y-%m-%dT%H:%M:%SZ')
            # entry['block'] = words[4]
            continue
        elif re.search('^.*UpdateTip.*$', line):
            words = line.split(' ')
            entry['tip_updated'] = parse_datetime(words[0])
            entry['block'] = words[3].replace('best=', '')
            entry['height'] = int(words[4].replace('height=', ''))
        elif re.search('cmpctblock', line):
            unhanded_lines.append(line)
        else:
            continue

        cache.append(entry)

log_df = pd.json_normalize(cache)
block_raw_df = log_df.groupby('block').min()
block_df = block_raw_df.assign(lag = block_raw_df['reconstructed'] - block_raw_df['initialized'])

print(block_df[block_df['height'] > 908000].sort_values('height'))

