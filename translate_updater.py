#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import mysql.connector
import mysql
from tqdm import tqdm


# In[2]:


translate_data_dir = '/home/waycool/Downloads/translate_data/'


# In[3]:


def db_connect():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            database="voice_assist_db",
            user="voice_assist",
            password="Waycool@12345")
        print('[info] : Database Connection Successful!!! :)')
        return mydb,1;
    except:
        print('[info] : Database Connection Failed.. :(')
        return False,0


# In[4]:


def get_newid_langid(l):
    #getting new id
    query="select max(vt_id) from iwap_va_translate"
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    new_id=[x[0] for x in mycursor]
    if new_id != [None]:
        new_id=int(new_id[0])+1
    else:
        new_id = 1

    # getting lang_id
    query="select vl_id from iwap_va_language where vl_name='{}';".format(l)
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    vl_id=[x[0] for x in mycursor]
    if vl_id !=[]:
        vl_id = vl_id[0]
    return new_id,vl_id


# In[5]:


def check_s_exists(s):
    query="select vt_id from iwap_va_translate where vt_spells='{}';".format(s)
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    if len([x[0] for x in mycursor]) != 1:
        return 0
    else:
        return 1


# In[6]:


def update_translate_table():
    languages=os.listdir(translate_data_dir)
    for l in languages:
        words = os.listdir(translate_data_dir+l)
        print('[info] : Updating words for',l,'language!!!!!!!!!!!!\nTotal no of words to check :',len(words))
        for w in tqdm(words):
            file=translate_data_dir+l+'/'+w
            word=w.split('.')[0]
            spellings=open(file,'r').read()
            lang_word=spellings.split('\n')[0]
            for s in spellings.split('\n'):
                ex=check_s_exists(s)
                if not ex and s != '':
                    new_id , lang_id= get_newid_langid(l)
                    query="insert into iwap_va_translate values ({},'{}',{},'{}','{}');".format(str(new_id),word,str(lang_id),lang_word,s)
                    mycursor = mydb.cursor(buffered=True)
                    mycursor.execute(query)
    mydb.commit()


# In[7]:


mydb,connected=db_connect()
if connected:
    update_translate_table()


# In[ ]:





# In[ ]:




