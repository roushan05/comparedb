#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gluon import *
class GenarateDbDiff(object):

    def __init__(self,firstdblist,seconddblist):
        self.firstdblist = firstdblist
        self.seconddblist = seconddblist
        self.FieldMisInProd = {}  # fields of table present in stage but not in prod
        self.FieldMisInStage = {}  # fields of table present in prod but not in stage
        self.TableMisInProd = {}  # tables present in stage but not in prod db
        self.TableMisInStage = {}  # tables present in prod but not in stage db
        self.DiffFieldType = {}  # fields present in both the tables but modified
        self.PrStageNtProd()
        self.PrProdNtStage()
        self.FldMissing()
        self.FldPrTypMissing()
        self.GenerateSql()

    def Compare(self,table):
        fieldsfirst = set("".join(j[0]) for j in self.firstdblist[table])
        fieldssecond = set("".join(j[0]) for j in self.seconddblist[table])
        self.FieldMisInProd[table] =  list(fieldsfirst - fieldssecond)
        self.FieldMisInStage[table] =  list(fieldssecond - fieldsfirst)
        if self.FieldMisInProd[table] == []:#delete emplty lists
            del self.FieldMisInProd[table]
        if self.FieldMisInStage[table] == []:
            del self.FieldMisInStage[table]

    def CompareFields(self,tablename,stage,prod):
        res = []
        indexa = ["".join(p[0]) for p in stage]
        indexb = ["".join(p[0]) for p in prod]
        for count, i in enumerate(indexa):
            try:
                indb = indexb.index(i)
                if stage[count] != prod[indb]:
                    res.append(stage[count])
            except ValueError:
                continue

        self.DiffFieldType[tablename] = res

    def PrStageNtProd(self):
        # tables present in stage but not in prod db
        for i in self.firstdblist:
            if i not in self.seconddblist:
                self.TableMisInProd[i]= self.firstdblist[i]
        for i in self.TableMisInProd:
            del self.firstdblist[i]
    def PrProdNtStage(self):
        # tables present in prod but not in stage db
        for i in self.seconddblist:
            if i not in self.firstdblist:
                self.TableMisInStage[i]= self.TableMisInStage[i]
        for i in self.TableMisInStage:
            del self.seconddblist[i]
    def FldMissing(self):
        #fields of table present in stage but not in prod and vice versa
        for table in self.firstdblist:
            self.Compare(table)
    def FldPrTypMissing(self):
        # fields of table present in both but types are  different
        for tablename in self.firstdblist:
            self.CompareFields(tablename,self.firstdblist[tablename],self.seconddblist[tablename])


    def GenerateSql(self):
        f = open("output.sql",'w')
        #create tables present in stage but not in prod
        if self.TableMisInProd != {}:
            for table in  self.TableMisInProd:
                statement = "create table"
                statement += " " + table + " ( "
                for i in self.TableMisInProd[table]:
                    statement +=  i[0] + ' ' + i[1]
                    if "PRI" in i :
                        statement+= " Primary key,"
                    else:
                        statement+= ","
                statement = statement.strip(",")
                statement += ');'
                f.write(statement)
                f.write("\n")
                #print statement
                statement =""
        #delete tables present in prod but not stage
        if self.TableMisInStage != {}:
            for table in self.TableMisInStage:
                statement = "Drop table "
                statement+= table  + ';'
                f.write(statement)
                f.write("\n")
                #print statement
                statement = " "
        #Add fields present in stage but missing in production
        if self.FieldMisInProd!={}:
            for table in self.FieldMisInProd:

                for field in self.FieldMisInProd[table]:

                    for i in self.firstdblist[table]:

                        if i[0] == field:

                            statement = "Alter table " + table + ' add ' + field + ' '+ i[1]+''
                            if i[3]=="PRI":
                                statement += " Primary key ;"
                            else:
                                statement += ';'
                            f.write(statement)
                            f.write("\n")
        #Delete fields present in prod but not in stage
        for table in self.FieldMisInStage:
            for fields in self.FieldMisInStage[table]:
                statement = "Alter table " + table +' drop ' + fields+";"
            f.write(statement)
            f.write("\n")
        #modify fields which are of different type
        for table in self.DiffFieldType:
            for fields in self.DiffFieldType[table]:
                statement =  "ALter table " + table + " modify column " + fields[0] + ' ' + fields[1]
                if fields[3] == "PRI":
                    statement+= " Primary key;"
                else:
                    statement+=';'
                f.write(statement)
                f.write("\n")

        f.close()
