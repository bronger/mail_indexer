#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, email, sqlite3, multiprocessing, pickle
from bs4 import BeautifulSoup


mail_filename_regex = re.compile(r"\d+$")
word_regex = re.compile("\w+")
message_id_regex = re.compile("<(.*?)>")

def build_custom_message_id(data):
    new_message_id = "{}-{}-{}".format(data["folder"], data["index"], data["message_id"])
    data["message_id"] = new_message_id
    return new_message_id

def get_body(message):
    payload = message.get_payload()
    text = ""
    if message.is_multipart():
        for part in payload:
            if part.get_content_type() == "multipart/alternative":
                text = get_body(part)
                break
            elif part.get_content_type() == "text/html":
                text = BeautifulSoup(part.get_payload()).get_text()
                break
            elif part.get_content_type() == "text/plain" and not text:
                text = part.get_payload()
    else:
        text = payload
    return text

def get_email(string):
    return email.utils.parseaddr(string)[1].lower()

def scan_recipients(field):
    if field:
        return {get_email(part) for part in str(field).split(",")}
    else:
        return set()

def process_chunk(filepaths):
    result = {}
    for path in filepaths:
        message = email.message_from_binary_file(open(path, "rb"))
        data = {}
        data["folder"] = os.path.basename(os.path.dirname(path))
        data["index"] = int(os.path.basename(path))
        data["message_id"] = "{}-{}@wilson.bronger.org".format(data["folder"], data["index"])
        if message["message-id"]:
            match = message_id_regex.search(message["message-id"])
            if match:
                data["message_id"] = match.group(1)
        data["subject"] = str(message["subject"] or "")
        data["sender"] = str(message["from"] or "")
        data["sender_email"] = get_email(data["sender"])
        data["recipients"] = " ".join(
            scan_recipients(message["to"]) | scan_recipients(message["cc"]) | scan_recipients(message["bcc"]))
        try:
            data["timestamp"] = message["date"] and email.utils.parsedate_to_datetime(message["date"])
        except TypeError:
            data["timestamp"] = None
        data["body"] = get_body(message)
        data["body_normalized"] = " ".join(word_regex.findall(data["body"])).lower()
        data["parent"] = None
        parent = message["in-reply-to"]
        if parent:
            match = message_id_regex.search(parent)
            if match:
                data["parent"] = match.group(1)
        if data["message_id"] in result:
            new_message_id = build_custom_message_id(data)
            result[new_message_id] = data
        else:
            result[data["message_id"]] = data
    return result


connection = sqlite3.connect(os.path.expanduser("~/Mail/mails.db"))
connection.execute("PRAGMA foreign_keys = 1")
connection.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS Mails USING fts4(
                          message_id CHARACTER(255), subject CHARACTER(255), body TEXT,
                          body_normalized TEXT, timestamp DATETIME, sender CHARACTER(255),
                          sender_email CHARACTER(255), recipients CHARACTER(1023),
                          folder CHARACTER(64), file_index INTEGER,
                          parent CHARACTER(255),
                          PRIMARY KEY (message_id),
                          FOREIGN KEY (parent) REFERENCES Mails(message_id))""")


print("Reading already seen mail data ...")
already_seen = set()
message_ids = set()
for message_id, folder, index in connection.execute("SELECT message_id, folder, file_index FROM Mails"):
    message_ids.add(message_id)
    already_seen.add((folder, index))
print("Searching for new mail files ...")
filepaths = []
for root, __, filenames in os.walk(os.path.expanduser("~/Mail")):
    folder = os.path.basename(root)
    for filename in filenames:
        if mail_filename_regex.match(filename):
            if (folder, int(filename)) not in already_seen:
                filepath = os.path.join(root, filename)
                filepaths.append(filepath)


print("{} new mails found.  Parsing mails ...".format(len(filepaths)))
chunksize = len(filepaths) // multiprocessing.cpu_count()
if len(filepaths) % multiprocessing.cpu_count():
    chunksize += 1
chunks = [filepaths[i * chunksize:(i + 1) * chunksize] for i in range(multiprocessing.cpu_count())]

messages = {}
pool = multiprocessing.Pool()
for result in pool.map(process_chunk, chunks):
    duplicates = message_ids.intersection(result)
    for duplicate in duplicates:
        try:
            data = messages[duplicate]
        except KeyError:
            data = {}
            data["message_id"], data["subject"], data["body"], data["body_normalized"], data["timestamp"], data["sender"], \
                data["sender_email"], data["recipients"], data["folder"], data["index"], data["parent"] = \
                    list(connection.execute("SELECT message_id, subject, body, body_normalized, timestamp, sender, "
                                            "sender_email, recipients, folder, file_index, parent FROM Mails "
                                            "WHERE message_id=?", (duplicate,)))[0]
        new_message_id = build_custom_message_id(data)
        messages[new_message_id] = data
    messages.update(result)
    message_ids = message_ids.union(messages)
pool.close()
pool.join()


print("Writing database ...")

def insert_data(data):
    connection.execute("INSERT INTO Mails (message_id, subject, body, body_normalized, timestamp, sender, sender_email, recipients, "
                       "folder, file_index, parent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       (data["message_id"], data["subject"], data["body"], data["body_normalized"], data["timestamp"], data["sender"],
                        data["sender_email"], data["recipients"], data["folder"], data["index"], data["parent"]))

def insert_message(data):
    try:
        insert_data(data)
    except sqlite3.IntegrityError:
        if data["parent"] in messages:
            insert_message(messages.pop(data["parent"]))
        else:
            data["parent"] = None
        insert_data(data)

while messages:
    insert_message(messages.popitem()[1])

connection.commit()
connection.close()
