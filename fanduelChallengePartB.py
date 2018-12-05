import pandas as pd
import numpy as np
import sqlite3 as sql3

# Database connection
conn = sql3.connect('sqlite.db')

event_summary = pd.read_sql_query("""SELECT * FROM event_summary""", conn)

conn.execute("""CREATE TABLE IF NOT EXISTS new_events (batch_id,to_email,sent_date,open_date,bounce_date,click_date)""")

conn.execute("""DELETE FROM new_events""")

conn.execute("""
    INSERT INTO new_events
    SELECT next.batch_id,next.to_email,next.sent_date,next.open_date,next.bounce_date,next.click_date
    FROM (SELECT table1.batch_id, table1.to_email, table1.sent_date, table2.open_date, table3.bounce_date, table4.click_date
    FROM send_event table1
    LEFT OUTER JOIN open_event table2 ON table1.batch_id=table2.batch_id AND table1.to_email=table2.to_email
    LEFT OUTER JOIN bounce_event table3 ON table1.batch_id=table3.batch_id AND table1.to_email=table3.to_email
    LEFT OUTER JOIN click_event table4 ON table1.batch_id=table4.batch_id AND table1.to_email=table4.to_email) new
    LEFT OUTER JOIN event_summary org ON next.batch_id=original.batch_id
    AND next.to_email=original.to_email
    AND (next.open_date=original.open_date OR (next.open_date IS NULL and original.open_date IS NULL))
    AND (next.sent_date=original.sent_date OR (next.sent_date IS NULL and original.sent_date IS NULL))
    AND (next.bounce_date=original.bounce_date OR (next.bounce_date IS NULL and original.bounce_date IS NULL))
    AND (next.click_date=original.click_date OR (next.click_date IS NULL and original.click_date IS NULL)) WHERE (original.to_email IS NULL AND original.to_email IS NULL
    AND original.open_date IS NULL
    AND original.sent_date IS NULL
    AND original.bounce_date IS NULL
    AND original.click_date IS NULL)""")


conn.execute("""DELETE FROM event_summary WHERE to_email IN (SELECT to_email FROM new_events) AND batch_id IN (SELECT batch_id FROM new_events) AND sent_date IN (SELECT sent_date FROM new_events)""")


conn.execute("""INSERT INTO event_summary SELECT * FROM new_events""")

cur.close()
conn.close();
