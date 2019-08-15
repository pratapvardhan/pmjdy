import os
import re
import sys
import logging
import argparse
import requests
import pandas as pd
from lxml.html import etree

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__file__)


URL = 'https://www.pmjdy.gov.in/archive'
_UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
       'AppleWebKit/537.36 (KHTML, like Gecko) '
       'Chrome/67.0.3396.99 Safari/537.36')
_DIR = os.path.dirname(os.path.abspath(__file__))
_APPDATA = os.path.join(_DIR, 'data')
_HTML = os.path.join(_APPDATA, 'html')
_CSV = os.path.join(_APPDATA, 'csv')
_session = requests.Session()


def make_dir(path):
    '''create folder path is it doesn't exist'''
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def _post(url, params, save):
    '''
    post a request and get the data, uses cache
    date parameter for cached path
    '''
    if not save:
        r = _session.post(url, data=params, headers={'User-Agent': _UA})
        return r.text
    date = params['ctl00$ContentPlaceHolder1$txtdate']
    date = pd.to_datetime(date, format='%d/%m/%Y')
    date = date.strftime('%Y-%m-%d')
    path = os.path.join(_HTML, date + '.html')
    if os.path.exists(path):
        log.debug('Reading %s from %s', date, path)
    else:
        log.debug('Fetching %s from %s', date, url)
        r = _session.post(url, data=params, headers={'User-Agent': _UA})
        with open(path, 'w') as fd:
            fd.write(r.text)
    return open(path).read()


def get_params():
    '''get form parameters for session use'''
    r = _session.get(URL, headers={'User-Agent': _UA})
    tree = etree.fromstring(r.text, etree.HTMLParser())
    # Get all input tags
    params = {x.attrib['name']: x.attrib.get('value', '')
              for x in tree.xpath('.//input')}
    return r.text, params


def get_page(date, params, save=True):
    '''get archive page for given date: 04/07/2018'''
    if not isinstance(date, str):
        date = date.strftime('%d/%m/%Y')
    params['ctl00$ContentPlaceHolder1$txtdate'] = date
    html = _post(URL, params=params, save=save)
    return html


def get_back():
    '''get pages going backward'''
    log.info('Start: reading pmjdy archive')
    make_dir(_HTML)
    make_dir(_CSV)
    html, params = get_params()
    last_date = re.search(r'{"endDate":"(.*)","format', html)
    last_date = pd.to_datetime(last_date.group(1) if last_date else 'now')
    # Last Wednesday
    date = last_date.replace(tzinfo=None) - pd.Timedelta(days=last_date.dayofweek-2)
    html = get_page(date, params)
    create_csv(date, html)
    end_date = pd.to_datetime('2014-09-20')
    while True:
        date -= pd.Timedelta(days=7)
        if date < end_date:
            log.info('Reached end of the tunnel')
            break
        html = get_page(date, params)
        create_csv(date, html)
    consolidate()


def create_csv(date, text):
    dfs = pd.read_html(text)
    if not isinstance(date, str):
        date = date.strftime('%Y-%m-%d')
    if len(dfs) != 6:
        log.error('File for %s has no (6) tables', date)
        return
    level = 'Summary'
    columns = dfs[2].columns
    data = []
    for df in dfs[2:]:
        _df = df.iloc[0:-1, :].copy()
        _df.columns = columns
        _df['level'] = level
        data.append(_df)
        level = df.iloc[-1, 0]
    data = pd.concat(data, ignore_index=False)
    data['date'] = date
    path = os.path.join(_CSV, date + '.csv')
    data.to_csv(path, index=False)
    log.debug('Created %s', path)
    return data, path


def consolidate():
    path = os.path.join(_APPDATA, 'data.csv')
    files = os.listdir(_CSV)
    data = pd.concat([
        pd.read_csv(os.path.join(_CSV, f)) for f in files
    ], ignore_index=True)
    data.to_csv(path, index=False)
    log.info('Master data created %s', path)
    return path


def parse_command_line(argv):
    '''Parse command line argument. See -h option
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--debug',
        help='Print lots of debugging statements',
        action='store_const', dest='loglevel', const=logging.DEBUG,
        default=logging.WARNING,
    )
    parser.add_argument(
        '-v', '--verbose',
        help='Be verbose',
        action='store_const', dest='loglevel', const=logging.INFO,
    )
    args = parser.parse_args()
    log.setLevel(args.loglevel)
    return args


def main():
    '''Main program. Sets up logging. Moves to actual work.'''
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                        format='%(name)s (%(levelname)s): %(message)s')
    try:
        parse_command_line(sys.argv)
        get_back()
    except KeyboardInterrupt:
        log.error('Program interrupted!')
    finally:
        logging.shutdown()


if __name__ == '__main__':
    sys.exit(main())
