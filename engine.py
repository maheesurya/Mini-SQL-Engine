import csv
import numpy
import sys
import os
import time
import re

        # Enter the database folder
database_folder = 'files/'
flag_print_extra = False
flag_print_output_col = True
flag_print_output_data = True
if flag_print_extra:
    flag_print_length=True
    flag_print_time = True
else:
    flag_print_length=False
    flag_print_time = False



def load_metadata(filename):
    attri = list()
    tinfo = {}

    try:
        f = open(filename,'r')
    except:
        print "Metadata file does not exist"
        exit(0)

    # lines = []
    # with open(filename,'r') as f:
    #     for line in f:
    #         lines.append(line)

    lines = f.readlines()
    flag = 0
    count = 0
    tname="a"

    for line in lines:
        line = line.split("\r\n");
        line = line[0]
        if line == "<begin_table>":
            flag = 1
            count = 0
        elif line == "<end_table>":
            count = 0
            flag = 0
        else:
            if flag==1:
                count = count+1
                if count==1:
                    tname=line
                    tinfo[tname] = []
                else:
                    temp = tname + "." + line
                    tinfo[tname].append(line)
                    attri = attri + [temp]
    return tinfo


        # parse queries

def parse_query(query):
    grp = ""
    req = {}

    que = query.split("select")
    if len(que)==2:
        if que[0] is not "":
            print "Error:- Refer manual for syntax"
            exit(0)
        que = que[1].split("from")
        if len(que)==2:
            req["select"]=[]
            req["select"]= req["select"] + (que[0].split(' '))
            req["select"] = ''.join(req["select"])
            que = que[1].split("where")
            if len(que)==1:
                req["from"] = []
                req["from"] = req["from"] + (que[0].split(' '))
                req["from"] = ''.join(req["from"])
            elif len(que)==2:
                req["from"] = []
                req["from"] = req["from"] + (que[0].split(' '))
                req["from"] = ''.join(req["from"])
                req["where"] = []
                req["where"] = req["where"] + (que[1].split(' '))
                req["where"] = ''.join(req["where"])
            else:
                print "Error:- Refer manual for syntax"
                exit(0)
        else:
            print "Error:- Refer manual for syntax"
            exit(0)
    else:
        print "Error:- Refer manual for syntax"
        exit(0)


    req["select"] = req["select"].split(',')
    req["from"] = req["from"].split(',')

    # print req
    return req

def load_tables(tables,tinfo):
    data = {}
    for table in tables:
        if table in tinfo.keys():
            data[table]={}
            for val in tinfo[table]:
                data[table][val] = []
        else:
            print "Error:- ", table," is not present in the database"
            exit(0)
    for table in tables:
        filename = table + ".csv"
        try:
            f = open(database_folder + filename,'r')
        except:
            print "Error:- ", table," is not present in the database"
            exit(0)
        lines = f.readlines()

        for line in lines:
            vals = line.split("\r\n")[0].split(',')
            count = 0
            for val in vals:
                data[table][tinfo[table][count]].append(int(val))
                count = count+1
    return data

def rec(project,data,tables,c_table,tinfo,counter):
    if(len(tables)==c_table):
        for table in tables:
            for val in tinfo[table]:
                project[table+'.'+val].append(data[table][val][counter[table]])
        return project

    table = tables[c_table]

    for val in tinfo[table]:
        temp = len(data[table][val])

    for i in range(0,temp):
        counter[table] = i
        project = rec(project,data,tables,c_table+1,tinfo,counter)

    return project


def combine_tables(data,tables,tinfo):
    project = {}
    counter = {}

    for table in tables:
        for val in tinfo[table]:
            project[table+'.'+val] = []
        counter[table] = 0

    project = rec(project,data,tables,0,tinfo,counter)

    return project

def apply_aggregate(joined_tables,oper,val,length):
    output = float('nan')
    for i in range(0,length):
        if i == 0:
            output = joined_tables[val][i]
            if oper=='count':
                output = 1
        else:
            if oper == 'min':
                output = min(output,joined_tables[val][i])
            elif oper == 'max':
                output = max(output,joined_tables[val][i])
            elif oper == 'sum':
                output = output + joined_tables[val][i]
            elif oper == 'avg':
                output = output + joined_tables[val][i]
            elif oper=='count':
                output = output+1
    if oper == 'avg' and length>0:
        output = (output*1.0)/length
        output = round(output,2)

    if oper=='count':
        output = length
    return output

def extract_col(req,joined_tables,tinfo,tables,col):
    col = col.split('(')
    col_final = ""
    for c in col:
        if c !="":
            col_final = c
            break
    col = col_final
    col = col.split(')')
    for c in col:
        if c !="":
            col_final = c
            break

    col= col_final

    if len(col.split('.'))==1:
        temp = ""
        flag = 0
        for table in tables:
            if col in tinfo[table]:
                flag=flag+1
                temp = table
        if(flag==0):
            print "Error :- '", col, "' is not present in the given list of tables"
            exit(0)
        elif(flag>1):
            print "Error :- '", col, "' is present in Multiple tables,please specify properly"
            exit(0)
        else:
            col = temp+'.'+col
    else:
        if col in joined_tables.keys():
            col = col
        else:
            print "Error :- '", col, "' is not present in the given list of tables"
            exit(0)

    return col

def evaluate(req,joined_tables,tinfo,tables,temp,func,i,duplicate_out):
    flag1=0
    flag2=0
    flag=0
    if(len(temp[0].split('('))>len(temp[0].split(')'))):
        flag=flag+(len(temp[0].split('('))-len(temp[0].split(')')))
        temp[0] = temp[0].split('(')
        col_final = ""
        for c in temp[0]:
            if c !="":
                col_final = c
                break
        temp[0] = col_final
        temp[0] = temp[0].split(')')
        for c in temp[0]:
            if c !="":
                col_final = c
                break

        temp[0]= col_final
    elif(len(temp[0].split(')'))>len(temp[0].split('('))):
        print "Error:- Improper condition writing"
        exit(0)

    try:
        val1 = int(temp[0])
    except:
        col1 = extract_col(req,joined_tables,tinfo,tables,temp[0])
        flag1=1
        val1 = joined_tables[col1][i]

    if(len(temp[1].split(')'))>len(temp[1].split('('))):
        flag=flag-(len(temp[1].split(')'))-len(temp[1].split('(')))
        temp[1] = temp[1].split('(')
        col_final = ""
        for c in temp[1]:
            if c !="":
                col_final = c
                break
        temp[1] = col_final
        temp[1] = temp[1].split(')')
        for c in temp[1]:
            if c !="":
                col_final = c
                break

        temp[1]= col_final
    elif(len(temp[1].split('('))>len(temp[1].split(')'))):
        print "Error:- Improper condition writing"
        exit(0)

    try:
        val2 = int(temp[1])
    except:
        col2 = extract_col(req,joined_tables,tinfo,tables,temp[1])
        flag2=1
        val2 = joined_tables[col2][i]

    val = True
    if func==">=":
        val = int(val1) >= int(val2)
    elif func=="<=":
        val = int(val1) <= int(val2)
    elif func=="==":
        val = int(val1) == int(val2)
        if(flag1==1 and flag2==1):
            temp_flag=0
            if(col1.split('.')[0]!=col2.split('.')[0]):
                for key in duplicate_out.keys():
                    if (col1 in duplicate_out[key]) or (col2 in duplicate_out[key]):
                        temp_flag = 1
                        if(col1 not in duplicate_out[key]):
                            duplicate_out[key].append(col1)
                        if(col2 not in duplicate_out[key]):
                            duplicate_out[key].append(col2)
                        break
                if temp_flag==0:
                    duplicate_out[col1] = []
                    duplicate_out[col1].append(col1)
                    duplicate_out[col1].append(col2)
    elif func=="!=":
        val = int(val1) != int(val2)
    elif func==">":
        val = int(val1) > int(val2)
    elif func=="<":
        val = int(val1) < int(val2)
    else:
        print "Error:- Invalid Operation"
        exit(0)


    return val,flag,duplicate_out

def apply_constraints(req,joined_tables,tinfo,tables):
    duplicate_out = {}
    if "where" not in req.keys():
        return joined_tables,duplicate_out


    parts  = req["where"]
    if len(parts.split('('))!= len(parts.split(')')):
        print "Unbalanced Bracketing"
        exit(0)
    list_oper = []
    list_words = []
    parts = parts.split("and")
    count = 0
    length = len(parts)
    for part in parts:
        if len(part.split("or"))>=2:
            part = part.split("or")
            length2 = len(part)
            count2 = 0
            for par in part:
                list_oper.append(par)
                if(count2!=length2-1):
                    list_words.append("or")
                count2=count2+1
            if(count!=length-1):
                list_words.append("and")
        else:
            list_oper.append(part)
            if(count!=length-1):
                list_words.append("and")
        count = count+1

    name = tables[0]+'.'+tinfo[tables[0]][0]
    length = len(joined_tables[name])
    project = {}
    poi=0

    for table in tables:
        for val in tinfo[table]:
            project[table+'.'+val] = []

    for i in range(0,length):
        count = 0
        count2 = 0
        ans = True
        val = True
        list_val = []
        list_count = []
        st_oper = []
        st_poi = -1
        list_count.append(0)
        list_val.append(ans)
        poi2=0
        flag=0
        for oper in list_oper:
            temp = oper
            func = ""
            if(len(oper.split("="))==2):
                if(len(oper.split(">="))==2):
                    temp = temp.split(">=")
                    func = ">="
                    val,flag,duplicate_out = evaluate(req,joined_tables,tinfo,tables,temp,func,i,duplicate_out)
                elif(len(oper.split("<="))==2):
                    temp = temp.split("<=")
                    func = "<="
                    val,flag,duplicate_out = evaluate(req,joined_tables,tinfo,tables,temp,func,i,duplicate_out)
                elif(len(oper.split("!="))==2):
                    temp = temp.split("!=")
                    func = "!="
                    val,flag,duplicate_out = evaluate(req,joined_tables,tinfo,tables,temp,func,i,duplicate_out)
                else:
                    temp = temp.split("=")
                    func = "=="
                    val,flag,duplicate_out = evaluate(req,joined_tables,tinfo,tables,temp,func,i,duplicate_out)
            elif(len(oper.split(">"))==2):
                temp = temp.split(">")
                func = ">"
                val,flag,duplicate_out = evaluate(req,joined_tables,tinfo,tables,temp,func,i,duplicate_out)
            elif(len(oper.split("<"))==2):
                temp = temp.split("<")
                func = "<"
                val,flag,duplicate_out = evaluate(req,joined_tables,tinfo,tables,temp,func,i,duplicate_out)
            else:
                print "Error:- Invalid Operation",temp
                exit(0)


            if(flag>=1):
                while(flag>0):
                    poi2 +=1
                    list_val.append(True)
                    list_count.append(0)
                    flag-=1
                list_val[poi2] = val
                list_count[poi2]=1
                if(count>0):
                    if(count2>len(list_words)-1):
                        print "Error:- Less number of and & or operations"
                        exit(0)
                    st_oper.append(list_words[count2])
                    st_poi+=1
                    count2 = count2+1
                count = count+1
            elif(flag<=-1):
                if(count2>len(list_words)-1):
                    print "Error:- Less number of and & or operations"
                    exit(0)
                if(count>0):
                    list_count[poi2]+=1
                    if list_words[count2] == "and":
                        list_val[poi2] = list_val[poi2] and val
                    elif list_words[count2]=="or":
                        list_val[poi2] = list_val[poi2] or val
                    else:
                        print "Error:- Invalid Operation",temp
                        exit(0)
                    count2 = count2 + 1
                else:
                    print "Error:- In writing Paranthesis"
                    exit(0)
                count = count + 1

                while(flag<0):
                    flag+=1
                    if(poi2==0):
                        print "Error:- Invalid paranthesis writing"
                        exit(0)
                    if(st_poi>-1):
                        if(st_oper[st_poi]=="and"):
                            list_val[poi2-1] = list_val[poi2-1] and list_val[poi2]
                            poi2-=1
                            st_poi-=1
                        elif(st_oper[st_poi]=="or"):
                            list_val[poi2-1] = list_val[poi2-1] or list_val[poi2]
                            poi2 = poi2-1
                            st_poi = st_poi-1
                        else:
                            print "Error:- Invalid Operation"
                            exit(0)
                        st_oper = st_oper[:-1]
                        list_val = list_val[:-1]
                    else:
                        list_val[poi2-1] = list_val[poi2]
                        list_val = list_val[:-1]
                        poi2 = poi2-1
            else:
                if(count>0):
                    if(count2>len(list_words)-1):
                        print "Error:- Less number of and & or operations"
                        exit(0)
                    if(list_words[count2] == "and"):
                        list_val[poi2] = list_val[poi2] and val
                    elif(list_words[count2] == "or"):
                        list_val[poi2] = list_val[poi2] or val
                    else:
                        print "Error:- Invalid Operation"
                        exit(0)
                    count2 = count2 +1
                else:
                    list_val[poi2] = val
                count = count + 1

            


            # print oper,flag
            # print list_val
            # print st_oper

            ### below implementation is independent of brackets, from left to right
            # if(list_count[poi2]==0):
            #     list_val[poi2]=val
            #     list_count[poi2]=list_count[poi2]+1
            #     count = count+1
            # else:
            #     if list_words[count2] == "and":
            #         list_val[poi2] = list_val[poi2] and val
            #     elif list_words[count2]=="or":
            #         list_val[poi2] = list_val[poi2] or val
            #     else:
            #         print "Error:- Invalid Operation",temp
            #         exit(0)
            #     count2 = count2+1
            #     count = count+1
            ###

        # print ans
        if poi2==0:
            if list_val[poi2]==True:
                for key in joined_tables.keys():
                    project[key].append(joined_tables[key][i])
                poi+=1

    # print list_oper
    # print list_words
    # print duplicate_out
    return project,duplicate_out


def show_output(req,joined_tables,tinfo,tables):
    cols = req["select"]
    list_out = []
    list_distinct = []
    list_aggre = {}
    list_aggre['max']=[]
    list_aggre['min']=[]
    list_aggre['sum']=[]
    list_aggre['avg']=[]
    list_aggre['count']=[]

    flag_main = -1
    for col in cols:
        if col=="":
            continue
        if col=='*':
            flag_main=0
            for table in tables:
                for val in tinfo[table]:
                    list_out.append(table+'.'+val)
        elif(len(col.split('max'))==2 or len(col.split('min'))==2 or len(col.split('sum'))==2 or len(col.split('avg'))==2 or len(col.split('count'))==2):
            if(flag_main==0):
                print "Error:- In Aggregated query, select list also contains non-aggregated columns"
                exit(0)

            if(len(col.split('max'))==2):
                col = col.split('max')[1]
                col = extract_col(req,joined_tables,tinfo,tables,col)
                list_out.append(col)
                list_aggre['max'].append(col)
            elif(len(col.split('min'))==2):
                col = col.split('min')[1]
                col = extract_col(req,joined_tables,tinfo,tables,col)
                list_out.append(col)
                list_aggre['min'].append(col)
            elif(len(col.split('sum'))==2):
                col = col.split('sum')[1]
                col = extract_col(req,joined_tables,tinfo,tables,col)
                list_out.append(col)
                list_aggre['sum'].append(col)
            elif(len(col.split('avg'))==2):
                col = col.split('avg')[1]
                col = extract_col(req,joined_tables,tinfo,tables,col)
                list_out.append(col)
                list_aggre['avg'].append(col)
            elif(len(col.split('count'))==2):
                col = col.split('count')[1]
                col = extract_col(req,joined_tables,tinfo,tables,col)
                list_out.append(col)
                list_aggre['count'].append(col)                
            else:
                print "Error:- Improper usage of Aggregate query"
                exit(0)
            flag_main=1
        elif(len(col.split('distinct'))==2):
            col = col.split('distinct')[1]
            if(flag_main==1):
                print "Error:- In Aggregated query, select list contains non-aggregated column -- ", col
                exit(0)
            elif(flag_main==2):
                print "Error:- Multiple distinct can't be used"
                exit(0)

            col = extract_col(req,joined_tables,tinfo,tables,col)
            list_out.append(col)
            list_distinct.append(col)
            flag_main = 2
        else:

            if flag_main==1:
                print "Error:- In Aggregated query, select list contains non-aggregated column -- ", col
                exit(0)
            if(flag_main==-1):
                flag_main=0
            col = extract_col(req,joined_tables,tinfo,tables,col)
            list_out.append(col)


    # print list_out
    if list_out==[]:
        print "Warning:- No select columns included"
        exit(0)

    joined_tables,duplicate_out = apply_constraints(req,joined_tables,tinfo,tables)

    for key in duplicate_out.keys():
        temp_flag = 0
        actual = ""
        for table in tables:
            for col2 in tinfo[table]:
                if (table+'.'+col2) in duplicate_out[key]:
                    actual = (table+'.'+col2)
                    temp_flag = 1
                    break
            if temp_flag==1:
                break

        for col in duplicate_out[key]:
            if col==actual:
                continue
            while col in list_out:
                list_out.remove(col)
    # print joined_tables

    if flag_main==0 or flag_main==2:
        dist_attri=""
        dist_attri_list= []
        if flag_main==2:
            dist_attri = list_distinct[0]
        count = 0
        temp = ""
        for val in list_out:
            if count==0:
                temp = val
            else:
                temp = temp + ','+val
            count = count+1
        if(flag_print_output_col):
            print temp

        name = tables[0]+'.'+tinfo[tables[0]][0]
        length = len(joined_tables[name])
        for i in range(0,length):
            flag=0
            temp = ""
            count = 0
            for val in list_out:
                if flag_main==2 and dist_attri==val:
                    if joined_tables[val][i] not in dist_attri_list:
                        dist_attri_list.append(joined_tables[val][i])
                    else:
                        flag = 1 
                if count == 0:
                    temp = str(joined_tables[val][i])
                else:
                    temp = temp + ',' + str(joined_tables[val][i])
                count = count+1
            if flag==0:
                if(flag_print_output_data):
                    print temp

    elif(flag_main==1):
        count = 0
        temp = ""
        for val in list_out:
            for oper in list_aggre.keys():
                if val in list_aggre[oper]:
                    val = oper +'('+val + ')'
                    if count==0:
                        temp = val
                    else:
                        temp = temp + ','+val
                    count = count+1
        if(flag_print_output_col):
            print temp

        name = tables[0]+'.'+tinfo[tables[0]][0]
        length = len(joined_tables[name])
        temp = ""
        count = 0
        for val in list_out:
            for oper in list_aggre.keys():
                if val in list_aggre[oper]:
                    output = apply_aggregate(joined_tables,oper,val,length)
                    if count==0:
                        temp = str(output)
                    else:
                        temp = temp + ',' + str(output)
                    count = count + 1
        if(flag_print_output_data):
            print temp
    if(flag_print_length):
        print "\n",length,"Rows in set"


    return

def process_query(req,tinfo):
    tables = req["from"]
    length = len(tables)
    flag=0
    for i in range(0,length):
        for j in range(i+1,length):
            if tables[i]==tables[j]:
                flag=1
                break
        if flag:
            break

    if(flag==1):
        print "Error:-table names should be unique"
        exit(0)

    data = load_tables(tables,tinfo)
    joined_tables = combine_tables(data,tables,tinfo)
    show_output(req,joined_tables,tinfo,tables)

    return

if(len(sys.argv)>2):
    print "Error:- Invalid command input"
    print 'Command should be of form python engine.py "statement"'
    exit(0)
queries = sys.argv[1].split(';')

if len(queries)==1 or queries[len(queries)-1]!="":
    print "Error :- Semicolon should be present at the end"
    exit(0)

        # Load Metadata file

tinfo = load_metadata(database_folder+'metadata.txt')
try:
    for query in queries:
        if query == "":
            continue
        start = time.time()
        query = re.sub(r'SELECT', 'select', query, flags=re.IGNORECASE)
        query = re.sub(r'WHERE', 'where', query, flags=re.IGNORECASE)
        query = re.sub(r'FROM', 'from', query, flags=re.IGNORECASE)
        query = re.sub(r'MAX', 'max', query, flags=re.IGNORECASE)
        query = re.sub(r'MIN', 'min', query, flags=re.IGNORECASE)
        query = re.sub(r'SUM', 'sum', query, flags=re.IGNORECASE)
        query = re.sub(r'COUNT', 'count', query, flags=re.IGNORECASE)
        query = re.sub(r'AVG', 'avg', query, flags=re.IGNORECASE)
        query = re.sub(r'DISTINCT', 'distinct', query, flags=re.IGNORECASE)
        query = re.sub(r'AND', 'and', query, flags=re.IGNORECASE)
        query = re.sub(r'OR', 'or', query, flags=re.IGNORECASE)
        # print query
        output = parse_query(query)
        process_query(output,tinfo)
        end = time.time()
        if(flag_print_time):
            print "Time taken:- ",end-start,"\n"
except:
    print "Error:- Inappropriate query"
    exit(0)
