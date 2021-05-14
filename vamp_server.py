#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import numpy as np
import os
import math
from datetime import date
from datetime import datetime
import requests
import flask
import sys
from flask import Flask, request, render_template
import mysql.connector
import mysql
import scipy.io.wavfile as s_wave
import speech_recognition as sr
from voice_configure import config_confirm,voice_data_dir,audio_save_path


# In[2]:


def crct_configuration():
    
    if config_confirm:
        if not os.path.exists(voice_data_dir):
            os.mkdir(voice_data_dir)
        if not os.path.exists(audio_save_path):
            os.mkdir(audio_save_path)
        return 1
    else:
        print('Please configure the paths and set "config_confirm = 1" in the - ',voice_data_dir,'.py file')
        return 0


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


def mp_translate(mydb,words):  # translate only in terms to market_price.. but not exactly to meaningful english.
    
    for w in range(0,len(words)):
        query="select vt_eng_word from iwap_va_translate where vt_spells='{}'".format(words[w])
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        trans_w=[x for x in mycursor]
        if trans_w == []:
            continue
        else:
            words[w] = trans_w[0][0]
    return words    


# In[5]:


def get_cat_market_loc_id_name_avail(mydb,trans_words):
    category_id='0'
    market_id='0'
    location_id='0'
    loc_market_id_list='0'
    for w in trans_words:
        #retrive id from category,market,location table finding the name match
        if category_id == '0':
            query="select vc_id,vc_name,vc_avail from iwap_vamp_category where vc_name='{}'".format(w)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            
            category_id_list=[x for x in mycursor]
            
            if category_id_list != []:
                x=category_id_list[0]
                category_id=[x[0],x[1],x[2]]                             
                                   
        if market_id == '0':
            query="select vm_id,vm_name,vm_avail,vl_id from iwap_vamp_market where vm_name='{}'".format(w)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            
            market_id_list=[x for x in mycursor]
            
            if market_id_list != []:
                x=market_id_list[0]
                market_id=[x[0],x[1],x[2]]
                
                #finding the location info:
                query="select vl_id,vl_name,vl_avail from iwap_vamp_location where vl_id={}".format(x[3])
                mycursor = mydb.cursor(buffered=True)
                mycursor.execute(query)
                
                location_id_list = [x for x in mycursor]
                                
                if location_id_list != []:
                    x = location_id_list[0]
                    location_id=[x[0],x[1],x[2]]
                    loc_market_id_list='0'         #reverting back the changes made when finding via: if location_id=='0'
 
        if location_id == '0':
            query="select vl_id,vl_name,vl_avail from iwap_vamp_location where vl_name='{}'".format(w)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)    
            
            location_id_list = [x for x in mycursor]
                                
            if location_id_list != []:
                x = location_id_list[0]
                location_id=[x[0],x[1],x[2]]
                
                if market_id == '0':
                    query="select vm_id,vm_name,vm_avail from iwap_vamp_market where vl_id={}".format(location_id[0])
                    mycursor = mydb.cursor(buffered=True)
                    mycursor.execute(query)
                    
                    loc_market_id_list=[x for x in mycursor]                
                    
    if location_id != '0' and market_id == '0':
        # if loc_maket_id_list == [] then no market available today
        # if len(loc_market_id_list) >= 2: then more than two market avail for that location specify markername.
        if len(loc_market_id_list) == 1:
            x=loc_market_id_list[0]
            market_id = x[0],x[1],x[2]
            loc_market_id_list = '0'
    
    return category_id,market_id,location_id,loc_market_id_list


# In[6]:


def market_price(mydb,category_id,market_id,location_id):
    '''query="select vi_id,vmp_price_perkg from iwap_vamp_market_price where vc_id={} and vm_id={} and vl_id={}".format(category_id,market_id,location_id)
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)'''
    
    query="select vi_id,vc_id,vm_id,vl_id,max(vmp_updatedAt) as vmp_updatedAt from (select * from iwap_vamp_market_price where vl_id=2 and vm_id=2 and vc_id=6) as T group by vi_id;"
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    x = [x for x in mycursor][0]

    query="select vi_id,vmp_price_perkg,vc_id,vm_id,vl_id,vmp_updatedAt,vmp_id from iwap_vamp_market_price where vi_id={} and vc_id={} and vm_id={} and vl_id={} and vmp_updatedAt='{}';".format(x[0],x[1],x[2],x[3],str(x[4]))
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    
    price_list={}
    for x in mycursor:
        print(x)
        query="select vi_name from iwap_vamp_item where vi_id={};".format(x[0])
        mycursor2 = mydb.cursor(buffered=True)
        mycursor2.execute(query)
        vi_name=[y[0] for y in mycursor2][0]
        price_list[vi_name]=x[1]
    return price_list


# In[7]:


def iwap_feature_market_price(mydb,trans_words,x1,y1):  # x1- latitude y1- longitude from farmer's geolocation info..
    category_id,market_id,location_id,market_id_list = get_cat_market_loc_id_name_avail(mydb,trans_words)
    print('ids : ',category_id,market_id,location_id,market_id_list)
    # Assigning the market nearest to the farmer's geoLocation.....
    if market_id == '0':
        if x1 != '' and y1 != '':             # Geolocation turned ON by userside..
            print('Searching for the nearest market price')
            
            # searching the nearest market..
            query="select vm_id,vm_latitude,vm_longitude,vm_name,vl_id from iwap_vamp_market where vm_avail='1'";
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            mycursor = [x for x in mycursor]
            
            # if not even one market is available(avail='1').. simply return
            if mycursor == []:
                message='No market is Available now..\nTry after some time..'
                if category_id != '0':
                    return {'data':{'min_price':'','max_price':''},'identified_keywords':{'category':category_id[1],'market':'','location':''},'question':'','message': message,'success':'0','response':'1'}
                else:
                    return {'data':{'min_price':'','max_price':''},'identified_keywords':{'category':'','market':'','location':''},'question':'','message': message,'success':'0','response':'1'}
            else:
                distance={}
                for x in mycursor:
                    x2,y2=float(x[1]),float(x[2])
                    distance[math.sqrt((x2-x1)**2   +   (y2-y1)**2)]=[x[0],x[3],'1',x[4]]     # distance between two points = sprt((x2-x1)^2+(y2-y1)^2)
                min_distance_market=distance[min(distance.keys())]


                # geting the location name from known market..
                query="select vl_id,vl_name from iwap_vamp_location where vl_id={}".format(min_distance_market[3]);
                min_distance_market.pop()       # removing the vl_id present at last in the list..
                mycursor = mydb.cursor(buffered=True)
                mycursor.execute(query)
                x = [x for x in mycursor][0]
                x=(np.array(x)).tolist()
                x.append('1')
                x[0]=int(x[0])
            
                market_id=min_distance_market
                location_id=x                  # 'x' is the min_distance location..
                print('Re: ids : ',category_id,market_id,location_id,market_id_list)
             
        else:             # User didnt turned On GeoLocation..
            message = 'Please tell market name.. (or)\nTurn on location to search nearest available Market..'
            if category_id != '0':
                return {'data':{'min_price':'','max_price':''},'identified_keywords':{'category':category_id[1],'market':'','location':''},'question':'','message': message,'success':'0','response':'1'}
            else:
                return {'data':{'min_price':'','max_price':''},'identified_keywords':{'category':'','market':'','location':''},'question':'','message': message,'success':'0','response':'1'}
     
    if market_id_list == []:
        return {'data':{'min_price':'','max_price':''},'identified_keywords':{'category':category_id[1],'market':market_id[1],'location':location_id[1]},'question':'','message': 'No market is available for location "{}"'.format(location_id[1]),'success':'0','response':'1'}

    elif len(market_id_list) >=2:
        return {'data':{'min_price':'','max_price':''},'identified_keywords':{'category':category_id[1],'market':market_id[1],'location':location_id[1]},'question':'','message': 'Please tell market name.\nSince more than one market is available for "{}"'.format(location_id[1]),'success':'0','response':'1'}

        
    if category_id != '0' and market_id != '0' and location_id != '0':
        if category_id[2] == '1' and market_id[2] == '1' and location_id[2] == '1':
            price_list = market_price(mydb,category_id[0],market_id[0],location_id[0])
            if price_list != {}:
                message='Price list:\n'+'\n'.join(str(price_list).split(','))[1:-1]
                message=message.replace("'","")
                print(message)
                min_price = min(price_list.values()) 
                max_price = max(price_list.values())
                print('[info] : Response Success!!!')
                return {'data':{'min_price':str(min_price),'max_price':str(max_price)},'identified_keywords':{'category':category_id[1],'market':market_id[1],'location':location_id[1]},'question':'','message':message,'success':'1','response':'1'}
            else:
                return {'data':{'min_price':'','max_price':''},'identified_keywords':{'category':category_id[1],'market':market_id[1],'location':location_id[1]},'question':'','message':'No item available for this category :"{}" at {} in {}'.format(category_id[1],market_id[1],location_id[1]),'success':'0','response':'1'}
        else:
            message = 'Your requested'
            if category_id[2] == '0':
                message = message + ' ' +category_id[1]+ ' category'
            if market_id[2] == '0':
                message = message + ' ' + market_id[1] + ' market'
            if location_id[2] == '0':
                message = message + ' ' + location_id[1] +' location'
            message = message + ' is not available today..\nPlease try others..\n'
            return {'data':{'min_price':'','max_price':''},'identified_keywords':{'category':category_id[1],'market':market_id[1],'location':location_id[1]},'question':'','message':message,'success':'0','response':'1'}
        
        
    else:
        message ='Please tell'
        if category_id == '0':
            message = message + ' ' + 'Category name'
        if market_id == '0':
            message = message + ' ' + 'Market name'
        if location_id == '0':
            message = message + ' ' + 'Location name'
            message = message +'\nSay "...Today Data Please.."\nto know the category,market,location list available today'
        return {'data':{'min_price':'','max_price':''},'identified_keywords':{'category':category_id[1] if category_id !='0' else '','market':market_id[1] if market_id !='0' else '','location':location_id[1] if location_id !='0' else ''},'question':'','message':message,'success':'0','response':'1'}


# In[8]:


def online(mydb,audio,farmer_id,language_code,feature_id,now,latitude,longitude):
    
    # getting last request id
    query="select max(vrd_id) from iwap_va_request_detail"
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    
    #new request id
    max_id=[x[0] for x in mycursor]
    if max_id == [None]:
        max_id = 0
    else:
        max_id = int(max_id[0])
    vrd_id=max_id+1
    
    
    #audio file path that where to be saved
    a_path = audio_save_path + 'speech_' + str(vrd_id)+'.wav'
    
    
    
    # Speech to Text conversion-----------------------------------------------------
    #with open(a_path,'wb') as audio_no:
        #audio_no.write(audio)
    #audio_no = wave.open(a_path,'wb')
    #audio_no.writeframesraw(audio)
    #sample_rate, data = s_wave.read(audio)
    #s_wave.write(a_path,sample_rate,data)
    
    r = sr.Recognizer()
    audio=sr.AudioFile(audio)
        
    with audio as source:
        audio = r.record(source)
    try:
        print('Processing audio...')
        spoken_text=r.recognize_google(audio, language=language_code)
        print("Spoken Text: "+ spoken_text)

    except sr.UnknownValueError:
        print("Couldn't understand")
        # response failed!!!!!!
        query="insert into iwap_va_request_detail values({},'{}','{}',{},{},{},'0',NULL)".format(str(vrd_id),now,a_path,farmer_id,feature_id,language_code)
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        
        return {'data': {},'identified_keywords':{},'question':'*********','message':"Could not understand your speech",'success':'0','response':'0'}
    except sr.RequestError as e:
        print("Couldn't request results; {}".format(e))
        # response failed!!!!!!
        query="insert into iwap_va_request_detail values({},'{}','{}',{},{},{},'0',NULL)".format(str(vrd_id),now,a_path,farmer_id,feature_id,language_code)
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        return {'data': {},'identified_keywords':{},'question':'*********','message':"Could not request your speech, Service not provided by Google",'success':'0','response':'0'}
    # ----------------------------------------------------------------------------------------
    
    
    
    #getting last response id
    query="select max(vrd_id) from iwap_va_response_detail;"
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    
    #new resonse id
    max_id=[x[0] for x in mycursor]
    if max_id == [None]:
        max_id = 0
    else:
        max_id = int(max_id[0])
    vrd_id_res=max_id+1
    
    #updating request table
    query="insert into iwap_va_request_detail values({},'{}','{}',{},{},'{}','1',{});".format(str(vrd_id),now,a_path,farmer_id,feature_id,language_code,str(vrd_id_res))  
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute(query)
    mydb.commit()
    #Pre-processing..
    words=spoken_text.upper().split()
    
    # Translator by searching and matching....-----------------------------------------------
    trans_words = mp_translate(mydb,words)
    trans_text = ' '.join(trans_words)
    trans_words = trans_text.split(' ')
    print('Translated Text : ',trans_text)
    # ---------------------------------------------------------------------------------------
    # iWAP market_price feature_id=1
    if feature_id == '1':
        
        post_reply=iwap_feature_market_price(mydb,trans_words,float(latitude),float(longitude))
        now=datetime.now()
        now=now.strftime("%y-%m-%d %H:%M:%S")
        message = post_reply['message']
        success = post_reply['success']
        
        #updating response table
        query="insert into iwap_va_response_detail values({},{},'{}','{}','{}',NULL,'{}','{}');".format(str(vrd_id_res),str(vrd_id),now,spoken_text,trans_text,message,success)  
        mycursor = mydb.cursor(buffered=True)
        mycursor.execute(query)
        mydb.commit() 
        
        #spoken_text=spoken_text.encode('utf-8')
        post_reply['question']=spoken_text 
        post_reply['translation']=trans_text
        return post_reply
    
    elif feature_id == '2':
        ## add service for new feature by this..
        pass
    
    else:
        print('Service is  not available for the requested iWAP feature!!!')
        


# In[9]:


def offline(f,farmer_id,language_code,feature_id,now):
    return {'data':{},'identified_keywords':{'category':'','market':'','location':''},'question':'!!!!!!','message':"Server couldn't able to connect the DataBase",'success_code':'0','response_code':'0'}


# In[11]:


app = Flask(__name__)
#app.config["DEBUG"] = True
@app.route('/v2/uploader', methods = ['GET','POST'])
def upload_file():

    # current time
    now=datetime.now()
    now=now.strftime("%y-%m-%d %H:%M:%S")

    if request.method == 'POST':
        audio = request.files['file']
        farmer_id = request.form['farmer_id']
        language_code = request.form['language_code']
        feature_id = request.form['feature_id']
        latitude = request.form['latitude']
        longitude = request.form['longitude']
        
        mydb,connected=db_connect()
        
        if connected:  
            # after checking DB connection.. other two(language and feature) factors need to be checked..
            #############################################################################################
            # check feature id is avail or not..
            query="select vf_avail from iwap_va_feature where vf_id={};".format(feature_id)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            feature_avail = [x for x in mycursor]
            
            if feature_avail == []:
                message ='Feature DB is empty!!\nHence no feature has service'
                return {'data':{},'identified_keywords':{},'question':'','message': message,'success':'0','response':'0'}
            else:
                feature_avail = int(feature_avail[0][0])
            
            # check langauage code is avail or not...
            query="select vl_avail from iwap_va_language where vl_lang_code='{}';".format(language_code)
            mycursor = mydb.cursor(buffered=True)
            mycursor.execute(query)
            language_avail = [x for x in mycursor]

            if language_avail == []:
                message='Language DB is empty!!\nHence no language is available'
                return {'data':{},'identified_keywords':{},'question':'','message': message,'success':'0','response':'0'}
            else:
                language_avail = int(language_avail[0][0])
            
            # when selected feature and language is present in DB and avail..  Then does the further processing..
            if feature_avail and language_avail:
                return online(mydb,audio,farmer_id,language_code,feature_id,now,latitude,longitude)
            
            # when language is present in DB but not avail today
            elif not language_avail:
                return {'data':{},'identified_keywords':{},'question':'','message':'Selected Language is not available today..\nPlease try other language..','success':'0','response':'0'}
            
            # when feature is present in DB but not avail today
            elif not feature_avail:
                return {'data':{},'identified_keywords':{},'question':'','message':'Select Feature is not available today..\nPlease try other language..','success':'0','response':'0'}
            
            # when selected language is not present in DB itself
            elif language_avail == []:
                return {'data':{},'identified_keywords':{},'question':'','message':'No such language..\nPlease try other language..','success':'0','response':'0'}
            # when selected feature is not present in DB itself
            elif feature_avail == []:
                return {'data':{},'identified_keywords':{},'question':'','message':'Service is not avail for your selected FEATURE available today..\nPlease try other language..','success':'0','response':'0'}
                
        else:
            return offline(audio,farmer_id,language_code,feature_id,now)    
        
if __name__ == '__main__':
    
    if crct_configuration():
        app.run()  # run our Flask app


# In[ ]:





# In[ ]:




