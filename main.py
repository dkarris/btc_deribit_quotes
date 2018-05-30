from deribit_api import RestClient
import requests
import csv
import time
import glob


client = RestClient("wJfEFYLawTLr", "KQFL4DPBLFKQ4X3MMFNUHJBTQUR37HBP")
url = "https://www.deribit.com"


def getinstruments(params = None, options = False):
  '''
  returns instruments names as list
  receives JSON from API in the format json ['result][..list of instruments with info]
  we only need instrument['instrumentName'] - so it returns only it
  params can be expired=true or false
    '''
  uri = "/api/v1/public/getinstruments"
  r = requests.get(url + uri, params=params)
  print ('Logging into: \n' + r.url)
  if r.status_code != 200:
    return ("Something wrong happened")
  elif not(options):
    # use lambda for practice - smth realy stupid and long map(filter). In python3 map return iterable
    # so use list. In python2 it was not necessary
    return (list(map(lambda instrument: instrument['instrumentName'],
            filter(lambda instrument: instrument['kind'] != 'option',
                    r.json()['result']))))
  else:
    #use list comprehension for practice
    return [instrument['instrumentName'] for instrument in r.json()['result'] if instrument['kind'] == 'option']


def getquotes(instrument_name):
    '''
    returns quotes for a given instrument
    '''
    quotes = []
    last_id = None # first call should be w/o lastId
    url = 'https://www.deribit.com/api/v1/public/getlasttrades?instrument=%s&count=10000&sort=desc' % (instrument_name)
    while True:
        print ('requesting\n'+url)
        r =requests.get(url)
        if r.status_code != 200:
            raise Exception("Server returned error %s" %r.status_code)
        result = r.json()
        if result['success'] != True or not(result['result']):
            return quotes
        if len(result['result']) == 0:
            return quotes
        #skip  first record
        iterquotes = iter(result['result'])
        next (iterquotes)
        for element in iterquotes:
            quotes.append(element)
        print ('Now quotes for instrument %s contains: %s recods' %(instrument_name, len(quotes)))
        if len(result['result']) < 10000:
            return quotes
        last_id = result['result'][-1]['tradeId']
        url = 'https://www.deribit.com/api/v1/public/getlasttrades?instrument=%s&count=10000&sort=desc&endId=%s' % (instrument_name,last_id)
        time.sleep(1)

def export_to_csv(quotes,filename):
    '''
    writes quotes to filename 
    '''
    with open(filename, 'w', newline='') as csvfile:
        #csvwriter = csv.writer(csvfile, delimiter=',',
        #                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
        csvwriter =  csv.DictWriter(csvfile, fieldnames = quotes[0].keys())
        csvwriter.writeheader()
        for quoteline in quotes:
            csvwriter.writerow(quoteline)

def generate_csv(instrument_list, overwrite_existing = False):
    ''' 
    generates csv file for each instrument in instrument_list.
    On overwrite existng True skips instrument if filename with such a name exists
    '''
    # get existing csv files list
    csv_files = [csvfile.split('.')[0].upper() for csvfile in glob.glob('*.csv')]
    for instrument in instrument_list:
        if not(overwrite_existing) and (instrument not in csv_files):
            print ('parsing instrument:' + instrument)
            quotes = (getquotes(instrument))
            if len(quotes)>0:
                export_to_csv(quotes,instrument.lower() + '.csv')
        else:
            # either we need overwrite file or smth else
            # TO DO add split for overwrite existing CSV files if needed
            # as of now will just display that instrument is skipped
            print ("Instrument: %s skipped. File already exists" % (instrument))
            
generate_csv(getinstruments({"expired": "true"}, options=False))
