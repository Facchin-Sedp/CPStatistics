# -*- coding: utf-8 -*-
import mysql.connector as mariadb
import sys
import csv
import os
import math
import subprocess
import datetime

directory = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')+"\\CPROVIDER STATISTICHE\\" + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
host='168.100.100.5'

def insert_in_CSV(array_Contacts,desc_censts,nomeCampagna):
   
   
    if not os.path.exists(directory):
        os.makedirs(directory) 
    
    f = open(directory+'\\'+nomeCampagna+' contacts '+desc_censts+'.csv','w')
    f.write('RIFTER;RIFPRA;NOME;PRIORITA;NUMTELEFONI;TELEFONO1\n')
    for line in array_Contacts:
        f.write(line+'\n') #Give your csv text here.
    ## Python will convert \n to os.linesep
    f.close()

def insert_in_CSV_Summary(rows,nomeCampagna):
   
   
    if not os.path.exists(directory):
        os.makedirs(directory)

    f = open(directory+'\\'+nomeCampagna+' summary.csv','w')
    f.write('CONT;STATO\n')
    for line in rows:
        f.write(line+'\n') #Give your csv text here.
    ## Python will convert \n to os.linesep
    f.close()

def insert_in_CSV_Temp(rows,nomeCampagna):
        if not os.path.exists(directory):
            os.makedirs(directory)

        f = open(directory+'\\'+nomeCampagna+' timing.csv','w')
      
        f.write('CONTATTO;TELEFONO;DATASTART;TIMESTART;TEMPO\n')
        for line in rows:
            f.write(line+'\n') #Give your csv text here.
        ## Python will convert \n to os.linesep
        f.close()


###################### MAIN ################

def main():
    #  main
    nomeCampagna = raw_input("Scrivi il nome della campagna (premi invio per tutte):")

    mariadb_connection = mariadb.connect(host=host,user='csfil', password='S1.fa', database='csfil')
    cursor = mariadb_connection.cursor()

    if(nomeCampagna.strip()!=""):

        query="""
            select cpa_id, cpa_rifter,cpg_campag,cpa_rifpra,cpa_nome,cpp_phonum,cpa_calsts "ESITO",cpp_calsts "ESITO PHONE",cpp_censts "ESITO CENTRALINO",cs_Descsts 
            from cpanagra,cpphones,cpstates,cpcamp
            where
            cpa_id=cpp_cpaid
            and cpg_id=cpa_cpgid
            and cpp_censts=cs_censts
            and cpp_censts=%s
            and cpg_campag='"""+nomeCampagna+"""'
            order by cpa_id desc, cpg_campag desc
            """
    else:
        query="""

            select cpa_id, cpa_rifter,cpg_campag,cpa_rifpra,cpa_nome,cpp_phonum,cpa_calsts "ESITO",cpp_calsts "ESITO PHONE",cpp_censts "ESITO CENTRALINO",cs_Descsts 
            from cpanagra,cpphones,cpstates,cpcamp
            where
            cpa_id=cpp_cpaid
            and cpg_id=cpa_cpgid
            and cpp_censts=cs_censts
            and cpp_censts=%s            
            order by cpa_id desc, cpg_campag desc
            """
    
    count=0
    for censts in range(6):
        cursor.execute(query, (censts,))
        array_Contacts=[]
        desc_censts=''
        nCampagna=''# memorizzo il cambio del nome
        for cpa_id,cpa_rifter, cpg_campag, cpa_rifpra, cpa_nome, cpp_phonum, cpa_calsts, cpp_calsts, cpp_censts, cs_Descsts in cursor:
            desc_censts=str(cpp_censts) + "-" + cs_Descsts
            array_Contacts.append(str(cpa_rifter)+";"+cpa_rifpra + ";"+cpa_nome.strip()+";1;1;"+cpp_phonum.strip())

            if(nCampagna==''):
                nCampagna=cpg_campag
                
            elif(nCampagna!=cpg_campag):
                insert_in_CSV(array_Contacts,desc_censts,nCampagna)
                array_Contacts=[]
                statistica_Summary(nCampagna);
                nCampagna=cpg_campag
                
        insert_in_CSV(array_Contacts,desc_censts,nCampagna)
        statistica_Summary(nCampagna);

        
        count+=1
        perc= float(count) / float(8) *100

        perc=math.ceil(perc)# arrotondo
        os.system('cls')  # lancio il cls 
        print(str(perc)+' %')
        ############################ 
    mariadb_connection.close()

    ####################### Solo da richiamare
    soloDaRichiamare(nomeCampagna);
    ##################################
    count+=1
    perc= float(count) / float(8) *100


    ####################### TEMPISTICHE
    tempistiche(nomeCampagna);
    ##################################

    count+=1
    perc= float(count) / float(8) *100
    perc=math.ceil(perc)# arrotondo
    os.system('cls')  # lancio il cls 
    print(str(perc)+' %')


    print("i file CSV si trovano in " + directory);


    subprocess.Popen('explorer "'+directory+'"')


    ## RIFTER;RIFPRA;NOME;PRIORITA';NUMTELEFONI;TELEFONO1;TELEFONO2;.......

def soloDaRichiamare(nomeCampagna):

    mariadb_connection = mariadb.connect(host=host,user='csfil', password='S1.fa', database='csfil')
    cursor = mariadb_connection.cursor()

    if(nomeCampagna.strip()!=""):

        query="""
            select cpa_id, cpa_rifter,cpg_campag,cpa_rifpra,cpa_nome,cpp_phonum,cpa_calsts "ESITO",cpp_calsts "ESITO PHONE",cpp_censts "ESITO CENTRALINO" 
            from cpanagra,cpphones,cpcamp
            where
            cpa_id=cpp_cpaid
            and cpg_id=cpa_cpgid            
            and cpa_calsts=3
            and cpp_censts not in(5,3,4)
            and cpg_campag='"""+nomeCampagna+"""'
            order by cpa_id desc, cpg_campag desc
            """
    else:
        query="""

            select cpa_id, cpa_rifter,cpg_campag,cpa_rifpra,cpa_nome,cpp_phonum,cpa_calsts "ESITO",cpp_calsts "ESITO PHONE",cpp_censts "ESITO CENTRALINO"
            from cpanagra,cpphones,cpstates,cpcamp
            where
            cpa_id=cpp_cpaid
            and cpg_id=cpa_cpgid
            and cpa_calsts=3
            and cpp_censts not in(5,3,4)            
            order by cpa_id desc, cpg_campag desc
            """

    cursor.execute(query)
    array_Contacts=[] 
    nCampagna=''# memorizzo il cambio del nome
    ############# LETTURA
    for cpa_id,cpa_rifter, cpg_campag, cpa_rifpra, cpa_nome, cpp_phonum, cpa_calsts, cpp_calsts, cpp_censts in cursor:

     
        array_Contacts.append(str(cpa_rifter)+";"+cpa_rifpra + ";"+cpa_nome.strip()+";1;1;"+cpp_phonum.strip())

        if(nCampagna==''):
            nCampagna=cpg_campag
                
        elif(nCampagna!=cpg_campag):
            insert_in_CSV(array_Contacts,desc_censts,nCampagna)
            array_Contacts=[]
            statistica_Summary(nCampagna);
            nCampagna=cpg_campag
    ##################### FINE 
    ####################### AGGREGO I CONTATTI ####################

    array_Aggregate=[]
    contnum=0 # conto i numeri di tel e parto da 1 per ogni contatto
    tempNomeContatto=""
    telefoni=""
    parsing_Contatto=[]
    
    for contatto in array_Contacts:
       
        parsing_Contatto=contatto.split(";")# riparisfico per comparare il nome
        
        nomeContatto=parsing_Contatto[2]# nome del contatto
        print(nomeContatto)
        if(nomeContatto==tempNomeContatto  or tempNomeContatto==""):# se è il primo giro o il contatto è uguale incremento il numtel
            contnum+=1
            telefoni+=parsing_Contatto[5]+";"# devo inserire i telefoni in coda
           
        else:# appena vedo che è diverso lo metto nel nuovo array con tutti i numeri di tel
            array_Aggregate.append(parsing_Contatto[0]+";"+parsing_Contatto[1] + ";"+tempNomeContatto+";1;"+str(contnum)+";"+telefoni)

            contnum=1# nuovo contatto quindi un telfono
            telefoni=parsing_Contatto[5]+";"# è il primo numero di telefono
            
        tempNomeContatto=nomeContatto
    
     # questo if serve perchè se non ci sono contatti da richiamare va in errore
    if(len(parsing_Contatto)>0):
        array_Aggregate.append(parsing_Contatto[0]+";"+parsing_Contatto[1] + ";"+tempNomeContatto+";1;"+str(contnum)+";"+telefoni)
    ###############################################################

    insert_in_CSV(array_Aggregate,"DA RICHIAMARE",nCampagna)


    mariadb_connection.close()

def statistica_Summary(nomeCampagna):
    mariadb_connection = mariadb.connect(host=host,user='csfil', password='S1.fa', database='csfil')
    cursor = mariadb_connection.cursor()
    

    query="""
        select count(*) Cnt,cs_descsts from cpanagra,cpphones,cpcamp,cpstates
        where
        cpa_id=cpp_cpaid
        and cpg_id=cpa_cpgid
        and cpg_campag='"""+nomeCampagna+"""'
        and cpp_censts=cs_censts
        group by cs_descsts
        order by 1 desc
        """
    cursor.execute(query)
    rows=[]

    for Cnt,cs_descsts in cursor:
        rows.append(str(Cnt) + ";"+ cs_descsts)
        insert_in_CSV_Summary(rows,nomeCampagna)


    mariadb_connection.close()

def tempistiche(Campagna):
    mariadb_connection = mariadb.connect(host=host,user='csfil', password='S1.fa', database='csfil')
    cursor = mariadb_connection.cursor()
    if(Campagna.strip()!=''):
        query="""

             select cpg_campag,trim(cpa_nome),cpp_phonum,cpl_seqcall,cpl_oracall,cpl_caldur from cpcamp,cpanagra,cpphones,cpcalls
             where 
             cpl_cpaid=cpa_id
             and
             cpa_cpgid=cpg_id
             and
             cpp_cpaid=cpa_id
             and 
             cpp_numpho=cpl_numpho
             and cpg_campag='"""+Campagna+"""'
             order by cpg_campag desc, cpl_caldur desc
        """
    else:
        query="""

        select cpg_campag,trim(cpa_nome),cpp_phonum,cpl_seqcall,cpl_oracall,cpl_caldur from cpcamp,cpanagra,cpphones,cpcalls
        where 
        cpl_cpaid=cpa_id
        and
        cpa_cpgid=cpg_id
        and
        cpp_cpaid=cpa_id
        and 
        cpp_numpho=cpl_numpho         
        order by cpg_campag desc, cpl_caldur desc
    """

    rows=[]
    nomeCampagna=''
    cursor.execute(query)
    for cpg_campag,cpa_nome,cpp_phonum,cpl_seqcall,cpl_oracall,cpl_caldur in cursor:

        rows.append(cpa_nome.strip()+";"+cpp_phonum.strip()+";"+cpl_seqcall+";"+cpl_oracall+";"+ str(cpl_caldur)+" ms.")

        if(nomeCampagna==''):
            nomeCampagna=cpg_campag              
        elif(nomeCampagna!=cpg_campag):
            insert_in_CSV_Temp(rows,nomeCampagna)
            rows=[]            
            nomeCampagna=cpg_campag
    insert_in_CSV_Temp(rows,nomeCampagna)
    mariadb_connection.close()

############################################### MAIN

if __name__ == "__main__":
    main()