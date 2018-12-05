import pandas as pd

import sqlite3

conn = sqlite3.connect("sqlite.db")

df = pd.read_sql_query("SELECT * FROM event_summary;", conn)

# print(df);

findDuplicates = pd.read_sql_query(
    """SELECT table1.*,table2.count FROM event_summary table1
LEFT OUTER JOIN (SELECT to_email, count(to_email) as count
FROM event_summary
GROUP BY to_email
HAVING count > 1) table2
ON table1.to_email=table2.to_email
WHERE count > 1""", conn)

# print(findDuplicates)
#duplicates come from left joining the click_event table
click_event = pd.read_sql_query("""SELECT * FROM click_event""", conn)

pand_merge = pd.merge(duplicates_in_event_summary,click_event[['to_email','click_date','user_agent','url']],how='left',on=['to_email','click_date'])

conn.execute("""CREATE TABLE IF NOT EXISTS final_table (batch_id,to_email,sent_date,open_date,bounce_date,click_count)""")
conn.execute("""DELETE FROM final_table""")
conn.execute("""
    INSERT INTO final_table
    SELECT batch_id,to_email,sent_date,bounce_date,open_date ,count(*) FROM event_summary GROUP BY to_email""")

final_table = pd.read_sql_query(
    """SELECT * FROM final_table""", conn)
print(final_table)

cur.close()
conn.close();
