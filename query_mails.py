#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3, argparse, os, datetime


parser = argparse.ArgumentParser(description="Query the mail DB.")
parser.add_argument("--newer", metavar="YYYY-MM", help="find only mails newer than that month")
parser.add_argument("--older", metavar="YYYY-MM", help="find only mails older than that month")
parser.add_argument("--body", metavar="PATTERN", help="string pattern for the body of the email")
parser.add_argument("--subject", metavar="SUBSTRING", help="substring for the subject of the email")
parser.add_argument("--from", dest="from_", metavar="EMAIL", help="sender's email address")
parser.add_argument("--to", metavar="EMAIL", help="one of the recipients' email address")
args = parser.parse_args()

connection = sqlite3.connect(os.path.expanduser("~/Mail/mails.db"))
components = []
if args.newer:
    year, __, month = args.newer.partition("-")    
    components.append(("timestamp >= ?", datetime.datetime(year=int(year), month=int(month), day=1)))
if args.older:
    year, __, month = args.older.partition("-")
    year, month = int(year), int(month)
    month += 1
    if month > 12:
        year += 1
        month = 1
    components.append(("timestamp < ?", datetime.datetime(year=int(year), month=int(month), day=1)))
if args.body:
    components.append(("body_normalized MATCH ?", args.body))
if args.subject:
    components.append(("subject LIKE ?", "%" + args.subject + "%"))
if args.from_:
    components.append(("sender_email LIKE ?", args.from_ + "%"))
if args.to:
    components.append(("recipients LIKE ?", "%" + args.to + "%"))

query_string = "SELECT folder, file_index FROM Mails"
if components:
    query_string += " WHERE " + " AND ".join(component[0] for component in components)
parameters = tuple(component[1] for component in components)
folders = {}
for folder, index in connection.execute(query_string, parameters):
    folders.setdefault(folder, set()).add(index)
for folder, indices in folders.items():
    print("{}: {}".format(folder, sorted(indices)))
