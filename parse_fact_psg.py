from lxml import objectify
from lxml import etree
import pandas as pd
import sqlalchemy as sa
import config as cfg
from time import time

time_start = time()
# Подключение к БД
ora = sa.create_engine('postgresql+psycopg2://'+cfg.user_db+':'+cfg.pass_db+'@'+'localhost/'+cfg.db)
conn = ora.connect()
col = ('object-code', 'object-type', 'date-hour', 'volume')
data = []

path = 'c:\develop\load_xml_psg\ATS_nczkd.xml'
parser = etree.XMLParser(encoding='windows-1251', remove_comments=True)
xml = objectify.parse(open(path), parser=parser)
root = xml.getroot()
for a in root.getchildren():
    for b in a.getchildren():
        for c in b.getchildren():
            q = {**b.attrib, **c.attrib}
            data.append(q)

# print(data)
df = pd.DataFrame(data, columns=col)
df['date'] = pd.to_numeric(df['date-hour'])//100
df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
df['hour'] = (pd.to_numeric(df['date-hour'])-100*(pd.to_numeric(df['date-hour'])//100))+1
del df['date-hour']
df['object-code'] = df['object-code'].astype(str)
df['object-type'] = pd.to_numeric(df['object-type'])
df['volume'] = pd.to_numeric(df['volume'])


print(df.dtypes)
# df.to_excel('c:\develop\load_xml\yxml.xlsx', index=False)
print(df)
df.to_sql('fact_eur', conn, if_exists='replace', index=False, chunksize=200, dtype={'object-code': sa.types.VARCHAR(10)})
print(round(time()-time_start,2), 'sec')
conn.close()