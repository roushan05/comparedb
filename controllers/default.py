# -*- coding: utf-8 -*-
# this file is released under public domain and you can use without limitations

# -------------------------------------------------------------------------
# This is a sample controller
# - index is the default action of any application
# - user is required for authentication and authorization
# - download is for downloading files uploaded in the db (does streaming)
# -------------------------------------------------------------------------
from  applications.comparedb.modules.DbDifference import *
#from new1 import *

def index():
    """
    example action using the internationalization operator T and flash
    rendered by views/default/index.html or views/generic.html

    if you need a simple wiki simply replace the two lines below with:
    return auth.wiki()
    """
    #response.flash = T("Hello World")
    return dict(message=T('A simple app to compare two databases'))


def user():
    """
    exposes:
    http://..../[app]/default/user/login
    http://..../[app]/default/user/logout
    http://..../[app]/default/user/register
    http://..../[app]/default/user/profile
    http://..../[app]/default/user/retrieve_password
    http://..../[app]/default/user/change_password
    http://..../[app]/default/user/bulk_register
    use @auth.requires_login()
        @auth.requires_membership('group name')
        @auth.requires_permission('read','table name',record_id)
    to decorate functions that need access control
    also notice there is http://..../[app]/appadmin/manage/auth to allow administrator to manage users
    """
    return dict(form=auth())


@cache.action()
def download():
    """
    allows downloading of uploaded files
    http://..../[app]/default/download/[filename]
    """
    return response.download(request, db)


def call():
    """
    exposes services. for example:
    http://..../[app]/default/call/jsonrpc
    decorate with @services.jsonrpc the functions to expose
    supports xml, json, xmlrpc, jsonrpc, amfrpc, rss, csv
    """
    return service()

def enter():
    form = SQLFORM.factory(Field('db1',
                                 label='Enter name of First DB?',
                                 requires=IS_NOT_EMPTY()),
                          Field('user1',
                                 label='Enter Username for First DB?',
                                 requires=IS_NOT_EMPTY()),
                           Field('password1',
                                 label='Enter Password for First DB?',type = "password",
                                 requires=IS_NOT_EMPTY()),
                           Field('db2',
                                 label='Enter name of Second DB?',
                                 requires=IS_NOT_EMPTY()),
                           Field('user2',
                                 label='Enter Username for Second DB?',
                                 requires=IS_NOT_EMPTY()),
                           Field('password2',
                                 label='Enter Password for Second DB?',type = "password",
                                 requires=IS_NOT_EMPTY()),
                          )
    form.element('input[name=(user1)')['_style']='width:150px'
    form.element('input[name=(db1)')['_style']='width:150px'
    form.element('input[name=(password1)')['_style']='width:150px'
    form.element('input[name=(user2)')['_style']='width:150px'
    form.element('input[name=(db2)')['_style']='width:150px'
    form.element('input[name=(password2)')['_style']='width:150px'
    
    string1 = ''
    string2 = ''
    if form.process().accepted:
        string1= 'mysql://'+ form.vars.user1 + ':' +form.vars.password1 + '@localhost/' + form.vars.db1
        string2= 'mysql://'+ form.vars.user2 + ':' +form.vars.password2 + '@localhost/' + form.vars.db2
        try:
            
            db1 = DAL(string1,
             pool_size=20,
             migrate_enabled=False,
             check_reserved=['all'])
            db2 = DAL(string2,
              pool_size=20,
              migrate_enabled=False,
              check_reserved=['all'])
            session.error = ""
            tables = {}
            tables = list(db1.executesql('show tables'))
            tablelist1=[element for tupl in tables for element in tupl]
            firstdblist = {}
            for table in tablelist1:
                query = "describe " + table
                firstdblist[table] = list(db1.executesql(query))
            tables = {}
            tables = list(db2.executesql('show tables'))
            tablelist2=[element for tupl in tables for element in tupl]
            seconddblist = {}
            for table in tablelist2:
                query = "describe " + table
                seconddblist[table] = list(db2.executesql(query))
            differ =GenarateDbDiff(firstdblist,seconddblist)
            session.FieldMisInProd =  differ.FieldMisInProd
            session.FieldMisInStage= differ.FieldMisInStage
            session.TableMisInProd = differ.TableMisInProd
            session.TableMisInStage = differ.TableMisInStage
            session.DiffFieldType = differ.DiffFieldType

        except Exception as e:
            session.error = "Unable to connect. Please enter valid Credentials."
            redirect(URL('second'))
        redirect(URL('second'))
    return dict(form=form)
def second():
    return dict()
