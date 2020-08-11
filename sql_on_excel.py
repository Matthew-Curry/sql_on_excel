"""
Created on Mon August 10th, 2020
A simple command line tool to allow execution of SQL on Excel and CSV files by constructing temporary SQLite Databases. Outputs results of queries to an excel file.

@author: Matthew Curry
"""

import sqlite3
import pandas as pd
import numpy as np
import argparse
import os
import shutil

def main(args):
    """Main method, uses command line inputs to interact with program. Will execute commands based on hiearchy defined in program entry point.
    Arguments:
        args: argparse.Namespace object that holds arguments passed in by the user."""
    # paths reletive to program's location on machine
    current_path = os.path.dirname(os.path.realpath(__file__))
    db_folder = os.path.join(current_path, "Databases")
    # execute user arguments in order of hiearchy
    if args.build_db_name is not None:
        build_db(args.build_db_name, current_path, db_folder)
    if args.delete_db_name is not None:
        delete_db_path(args.delete_db_name, db_folder)
    if args.file_to_import_args is not None:
        import_file_to_db(args.file_to_import_args, db_folder)
    if args.query_to_execute is not None:
        execute_query(args.query_to_execute, db_folder)
    if args.clear_all_data:
        shutil.rmtree(db_folder)
        print('Data from all SQlite Databases are deleted.')
    if args.list_all_db_name:
        list_db(db_folder)
    if args.list_table_db_name:
        list_tables(args.list_table_db_name, db_folder)

def build_db(db_name, current_path, db_folder):
    """A function that builds a SQLite DB with the given name. Will build the Database in a folder called "Databases" in the directory where this program is saved.
    Arguments:
        db_name: the name of the db to create
        current_path: the directory where this program lives
        db_folder: where the folder with DB files shoule be located
    """
    # if "Databases" folder does not exist, create it
    if os.path.exists(db_folder) == False:
        os.makedirs(db_folder)
    db_file = db_name + ".db"
    db_path = os.path.join(db_folder, db_file)
    # make the db
    conn = sqlite3.connect(r"{}".format(db_path))
    conn.close()
    print('Successfully created SQLite Database ', "'{}'".format(db_file)) 

def delete_db_path(db_name, db_folder):
    """Delete the given DB name
    Arguments:
        db_name: the name of the DB to delete
        db_folder: where the folder of Database files resides
    """
    # Full file path of DB
    db_file = db_name + ".db"
    full_db_path = os.path.join(db_folder, db_file)
    try:
        os.remove(full_db_path)
        print('Successfully deleted SQLite Database ', "'{}'".format(db_file))
    except(FileNotFoundError):
        error_str = 'There is no database with the name ' + db_name + '. Did you specify the correct name of the Database you want to delete?'
        raise Exception(error_str)

def import_file_to_db(import_file_args, db_folder):
    """Parses import_file argument to import csv or excel files into the passed in db.
    Arguments:
        import_file_args: arguments passed in to the import file command in a list. First position is the name of the DB, second is the file, third is the name of the table,
                            and fourth is an optional argument for XLSX files for the sheetname to read in the table from.
        db_folder: the folder where the DB files are stored
        ***TO READ DATA IN PROPERLY, TABLE NEEDS TO START IN THE FIRST COLUMN AND FIRST ROW SHOULD BE COLUMN HEADERS"""
    # user args
    db_name = import_file_args[0]
    data_path = import_file_args[1]
    table_name = import_file_args[2]
    # check that table name is valid
    check_sqlite_entity_syntax(table_name, "Table")
    # read in the data as a DataFrame
    data_path = r"{}".format(data_path)
    if data_path[-3:] == 'csv':
        if import_file_args[2]:
            raise TypeError('Cannot pass in a sheet name with a CSV file')
        data = pd.read_csv(data_path)
    elif data_path[-4:] == 'xlsx':
        if len(import_file_args) == 4:
            data = pd.read_excel(data_path, sheet_name = import_file_args[3])
        else:
            data = pd.read_excel(data_path)
    else:
        raise TypeError("Unsuported File extension. Supported formats are .xlsx and .csv")
    # check that column names are valid. 
    for col in data.columns:
        check_sqlite_entity_syntax(col, "Column")
    # import Dataframe to the intended Database
    db_name = import_file_args[0]
    db_file = db_name + ".db"
    db_path = os.path.join(db_folder, db_file)
    db_exists = False
    for db_file_name in os.listdir(db_folder):
        if db_file_name == db_file:
            db_exists = True
    if db_exists:
        conn = sqlite3.connect(db_path)
    else:
        error_str = 'There is no database with the name ' + db_name + '. Did you create the database before importing the file?'
        raise Exception(error_str)
    data.to_sql(name=import_file_args[2], con=conn, if_exists='fail')
    conn.close()
    print('Successfully imported file ', "'{}'".format(import_file_args[1]), 'to', "'{}'".format(db_file))

def execute_query(execute_query_args, db_folder):
    """executes a given query against a given DB
    Arguments:
        execute_query_args: list holding arguments passed in from user. First arg is the query as a str or in a .txt file, second is the folder to save the result query,
                            third is the name to save the result file as, fourth is the DB to run the query on, last is an optional argument that if specified as "clear" will 
                            clear the DB the query was run on
        db_folder: folder where the SQLite DB files are stored."""
        
    # parameters from the user
    query = get_query(execute_query_args[0])
    output_dir = execute_query_args[1]
    output_name = execute_query_args[2] + '.xlsx'
    db_name = execute_query_args[3]
    # the file
    db_file = db_name + ".db"
    db_path = os.path.join(db_folder, db_file)
    try:
        conn = sqlite3.connect(db_path)
    except(sqlite3.OperationalError):
        error_str = 'There is no database with the name ' + db_name + '. Did you create the database before running the query?'
        raise Exception(error_str)
        
    # connect and execute
    output_path = os.path.join(output_dir, output_name)
    try:
        result = pd.read_sql_query(query, conn)
    except(pd.io.sql.DatabaseError):
        error_str = 'There is an error in the query. Did you import the table from an excel file into ' + db_name +'?'
        raise Exception(error_str)
    if 'index' in result.columns:
        result.drop('index', inplace = True, axis = 1)
    result.to_excel(output_path, index = False)
    conn.close()
    print('Query ran succesfully. Output found at', output_path)
    
    # clear the current Database if argument supplied
    if len(execute_query_args) == 5:
        if execute_query_args[4] == "clear":
            delete_db_path(db_name, db_folder)
        else:
            print('The fifth argument', execute_query_args[4], 'is not understood, so know data was deleted. Can supply fourth positional argument "clear" to --execute_query to delete the Database after running a query')

def get_query(user_query):
    """helper method with logic to get the query from the user. Will return the query if given in command line. If given a .txt file, will read the query from the file
        and return"""
    if user_query[-4:] == '.txt':
        with open(user_query,'r') as fh:
            all_lines = fh.readlines()
        if len(all_lines) == 0:
            raise Exception('The given file holding the query is empty.')
        user_query = ''.join(all_lines)
    return user_query 

def check_sqlite_entity_syntax(entity, type_):
    """SQLite Tables and columns follow same guidlines. Helper method that checks if a given object and its type as strings follows syntax, then prints appropriate Exception.
        type_ expected to be capitalized"""
    if entity.isdigit():
        raise Exception('Invalid ' + type_.lower() + ' ' + entity + '.' + ' ' + type_ + ' name cannot lead with a digit')
    # otherwise, should be all alphanumeric characters. Can have _ as well.
    entity_check = entity.replace("_", "A")
    if not entity_check.isalnum():
        raise Exception('Invalid ' + type_.lower() + ' ' + entity + '.' + ' '+ type_ + ' must contain only numbers and letters.')

def list_db(db_folder):
    """Lists all SQLite Databases the program knows about"""
    for db in os.listdir(db_folder):
        print(db)

def list_tables(db_name, db_folder):
    """Lists all databases in the given SQLite Database"""
    db_file_name = db_name + '.db'
    db_path = os.path.join(db_folder, db_file_name)
    try:
        conn = sqlite3.connect(db_path)
    except(sqlite3.OperationalError):
        error_str = 'There is no database with the name ' + db_name + '. Did you create the database before running the query?'
        raise Exception(error_str)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print(cursor.fetchall())
    cursor.close()
    conn.close()

if __name__ == '__main__':
    # primary arg parser
    parser = argparse.ArgumentParser(description = '''A simple command line tool to allow execution of SQL on Excel and CSV files. Has commands to build and delete in memory 
                                                     SQlite DBs to store files added, commands to add tables from csv and XLSX extensions, as well as specific sheets in 
                                                     an XLSX file, and commands to execute SQL against one of these SQLite DBs. If you run multiple arguments in a command, 
                                                     they will run according to the following order hierchy:
                                                     
                                                     build_db
                                                     delete_db
                                                     import_file
                                                     execute_query
                                                     list_all_tables
                                                     list_all_data
                                                     clear_all_data
                                                     .''')
    
    parser.add_argument('-b','--build_db',
                        metavar = 'build_db_name', 
                        dest='build_db_name', 
                        help='Enter the name of the SQLite DB to make',
                        type = str)
    parser.add_argument('-d','--delete_db',
                        metavar = 'delete_db_name', 
                        dest='delete_db_name', 
                        help='Enter the name of the SQLite DB to delete',
                        type = str)
    parser.add_argument('-i','--import_file',
                        metavar = 'file_to_import_args', 
                        dest='file_to_import_args', 
                        help="""Enter the name of the Database you want to add a file to. Then, type the full path to the file you want to add. The third argument is what you 
                        want the table name to be. You can then add an optional fourth agument that represents a sheetname to take data from an xlsx file. All data imported should have 
                        the names of the columns in the first row, and no other data in the file other than the table to import.""",
                        type = str,
                        nargs='+')
    parser.add_argument('-e','--execute',
                        metavar = 'execute_query', 
                        dest='query_to_execute', 
                        help="""Enter the query you want to run, or path to a .txt file holding the query you want to run, followed by the directory that you want the executed
                                query to go as a saved excel file, followed by the name of the excel file output (no extention), followed by the SQlite DB to run the query 
                                against. An optional fifth argument can be entered by typing "clear" which will clear the Database that the query is run against, as this is 
                                intended to be a lightweight tool. 
                                
                                The query should be surrounded by double quotation marks if given on command line. If given by .txt file, make sure the file only have the query
                                in its text. Make sure the Database has been created before you try to execute the query, which can be done with the -b [database_name] command""",
                        type = str,
                        nargs='+')
    parser.add_argument('-lt','--list_all_tables', 
                        metavar = 'list_table_db_name',
                        dest='list_table_db_name', 
                        help="""Lists all tables for a given SQLite Database that the program is aware of. Takes the name of the DB without extension.""")
    parser.add_argument('-ld','--list_all_db_name', 
                        dest='list_all_db_name', 
                        help="""Lists all SQLite Databases that the program is aware of.""",
                        action='store_true')
    parser.add_argument('-c','--clear_all_data', 
                        dest='clear_all_data', 
                        help="""Type "True if you would like to delete all data in the Databases folder.""",
                        action='store_true')
    args = parser.parse_args()
    # send all arguments to the main method to deal with
    main(args)