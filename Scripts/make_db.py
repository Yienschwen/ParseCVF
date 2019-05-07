#!/usr/bin/env python3

import sqlite3
import requests
import argparse
from sys import argv
import queue
import threading
import warnings
import colorama
from math import log10, floor

from Parsers import *


class TinyProg:
    def __init__(self, max_val, on_update=None):
        self.max_val = max_val
        self.value = 0
        self._lock = threading.RLock()
        if on_update is not None:
            self._on_update = on_update
        else:
            self._on_update = lambda vval, mmax: None

    def increment(self, step=1):
        self._lock.acquire()
        try:
            self.value += step
            self._on_update(self.value, self.max_val)
        finally:
            self._lock.release()
        return self.value


detail_prog = None


def CreateDB(path):
    try:
        conn = sqlite3.connect(path)
        conn.execute('''
            CREATE TABLE thumbnails
            (conf text, year text, idx integer, title text, link text)
        ''')
        conn.execute('''
            CREATE TABLE authors
            (conf text, year text, idx integer, author text)
        ''')
        conn.execute('''
            CREATE TABLE abstracts
            (conf text, year text, idx integer, abstract text)
        ''')
        conn.execute('''
            CREATE TABLE links
            (conf text, year text, idx integer, pdf text, Supp text, arXiv text)
        ''')
    except sqlite3.OperationalError:
        pass
    return conn


def GetThumbnails(link):
    thumb_parse = ThumbnailParser()
    with requests.get(link) as r:
        r.close()
        print('GET', 'THUMBNAILS', r.status_code, link)
        if r.ok:
            thumbnails = thumb_parse.feed(link, r.text)
            print('OK', 'THUMBNAILS', 'LEN:', len(thumbnails))
            return thumbnails
    raise RuntimeError('LIST HTML: {}'.format(link))


def WriteThumbnails(db, thumbnails, conf, year):
    db.executemany('INSERT INTO thumbnails VALUES ("{}", "{}", ?, ?, ?)'.format(
        conf, year), [(idx, thumbnail.title, thumbnail.link) for idx, thumbnail in enumerate(thumbnails)])
    db.commit()


def GetOneDetail(thumbnail):
    # print(detail_prog)
    with requests.get(thumbnail.link) as r:
        r.close()
        detail_prog.increment()
        # print('GET', 'DETAIL', r.status_code,
        #       thumbnail.idx, thumbnail.link)
        if r.ok:
            return DetailParser().feed(thumbnail.link, r.text)
    print('\r\n\n' + colorama.Fore.RED + "FAILED TO GET DETAIL:\n" +
          str(thumbnail) + colorama.Style.RESET_ALL + '\n')
    return None


def DetailWorker(results, thumb_queue):
    while True:
        thumb = thumb_queue.get()
        if thumb is None:
            break
        results[thumb.idx] = GetOneDetail(thumb)
        thumb_queue.task_done()


def GetDetails(thumbnails, n_pool=16):
    details = [None] * len(thumbnails)
    thumb_queue = queue.Queue()
    for thumb in thumbnails:
        thumb_queue.put(thumb)
    threads = list()
    N_THREADS = 16
    for _ in range(N_THREADS):
        t = threading.Thread(target=DetailWorker, args=(details, thumb_queue))
        t.start()
        threads.append(t)
    thumb_queue.join()
    for _ in range(N_THREADS):
        thumb_queue.put(None)
    for t in threads:
        t.join()
    return [(idx, detail) for idx, detail in enumerate(details) if detail is not None]


def WriteDetails(db, details, conf, year):
    authors = [(idx, author) for (idx, detail) in details
               for author in detail.authors]
    db.executemany(
        'INSERT INTO authors VALUES ("{}", "{}", ?, ?)'.format(conf, year), authors)
    db.commit()
    abstacts = [(idx, detail.abstract) for idx, detail in details]
    db.executemany(
        'INSERT INTO abstracts VALUES ("{}", "{}", ?, ?)'.format(conf, year), abstacts)
    db.commit()
    links = [(idx, detail.pdf, detail.Supp, detail.arXiv)
             for idx, detail in details]
    db.executemany('INSERT INTO links VALUES ("{}", "{}", ?, ?, ?, ?)'.format(conf, year), links
                   )
    db.commit()


def ConstructUpdate(llen):
    n_digits = int(floor(log10(llen))) + 1
    fmt = '%0{}i'.format(n_digits)
    fmt = fmt + ' / ' + fmt
    return lambda vval, _: print('GET', 'DETAILS', fmt % (vval, llen), end='\r')


def Build(link, conf, year, path):
    global detail_prog
    colorama.init()
    if path is None:
        path = '{}.{}.db'.format(conf, year)
        print('Using default db path {}'.format(path))
    db = CreateDB(path)
    thumbnails = GetThumbnails(link)
    WriteThumbnails(db, thumbnails, conf, year)
    detail_prog = TinyProg(len(thumbnails), ConstructUpdate(len(thumbnails)))
    details = GetDetails(thumbnails)
    WriteDetails(db, details, conf, year)
    db.commit()
    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Create a SQLite database of papers from CVF Open Access link')
    parser.add_argument('--link', required=True,
                        help='Link to CVF Open Access Conference')
    parser.add_argument('--conf', required=True, help='Name of conference')
    parser.add_argument('--year', required=True, help='Year of conference')
    parser.add_argument('--path', help='Path of output database')
    # print(parser.parse_args(argv[1:]))
    args = parser.parse_args(argv[1:])
    Build(args.link, args.conf, args.year, args.path)
