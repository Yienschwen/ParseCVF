#!/usr/bin/env python3

import sqlite3
import requests
import argparse
from sys import argv
import multiprocessing
import warnings
import colorama

from Parsers import *


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
            return thumb_parse.feed(link, r.text)
    raise RuntimeError('LIST HTML: {}'.format(link))


def WriteThumbnails(db, thumbnails, conf, year):
    db.executemany('INSERT INTO thumbnails VALUES ("{}", "{}", ?, ?, ?)'.format(
        conf, year), [(idx, thumbnail.title, thumbnail.link) for idx, thumbnail in enumerate(thumbnails)])
    db.commit()


def GetOneDetail(thumbnail):
    with requests.get(thumbnail.link) as r:
        r.close()
        print('GET', 'DETAIL', r.status_code,
              thumbnail.idx, thumbnail.link)
        if r.ok:    
            return DetailParser().feed(thumbnail.link, r.text)
    print(colorama.Fore.RED + "\n\nFAILED TO GET DETAIL:\n" +
          str(thumbnail) + '\n' + colorama.Style.RESET_ALL)
    return None


def GetDetails(thumbnails, n_pool=16):
    pool = multiprocessing.Pool(n_pool)
    details = pool.map(GetOneDetail, thumbnails)
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


def Build(link, conf, year, path):
    colorama.init()
    if path is None:
        path = '{}.{}.db'.format(conf, year)
        print('Using default db path {}'.format(path))
    db = CreateDB(path)
    thumbnails = GetThumbnails(link)
    WriteThumbnails(db, thumbnails, conf, year)
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
