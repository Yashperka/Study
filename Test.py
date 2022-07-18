import pandas as pd
from datetime import datetime, timedelta
import sqlite3
from memory_profiler import profile

@profile
def load_errors(Date):
    #Считываем данные и файлов в Dataframe pandas
    server_df = pd.read_csv("server.csv")
    client_df = pd.read_csv("client.csv")
    
    #Приводим столбцы дат к типу datetime
    server_df.timestamp = pd.to_datetime(server_df.timestamp, unit="s")
    client_df.timestamp = pd.to_datetime(client_df.timestamp, unit="s")

    #Загружаем таблицу cheaters
    conn = sqlite3.connect("cheaters.db")
    cheaters_df = pd.read_sql("SELECT * FROM cheaters", conn, parse_dates="ban_time")

    #Объединяем таблицы
    #Поскольку в итоговой таблице должны быть заполнены поля server_timestamp и player_id то делаем inner join, хотя и потеряем часть данных
    #На самом деле немного странно, что при почти одинаковом количестве записей(Server 66675, Client 66679) обе таблицы на одну половину состоят
    #из своих, а на другую из общих по Error_id записей
    joined_df = client_df.merge(server_df, on='error_id', how ='inner', suffixes=('_client', '_server'))

    #Выбираем данные за дату Date
    Date1 = (datetime.strptime(Date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    query = "'"+Date+"'" + " <= timestamp_server <" + "'"+Date1+"'" 
    joined_df = joined_df.query(query)

    #Объединяем таблицу с читерами
    result_df = joined_df.merge(cheaters_df, on='player_id', how ='left')
    result_df.ban_time = result_df.ban_time.fillna(datetime.today())

    #Удаляем часть читеров по условию
    result_df = result_df[result_df["ban_time"] + timedelta(days=1) > result_df["timestamp_server"]]
   
    #Создаем итоговую таблицу
    conn = sqlite3.connect("results.db") 
    res_cols = ["timestamp", "player_id", "event_id", "error_id", "json_server", "json_client"]
    rename_dict = {"timestamp_server" : "timestamp", "description_server" : "json_server", "description_client" : "json_client"}
    result_df.rename(columns= rename_dict, inplace= True)
    result_df[res_cols].to_sql("results", conn, if_exists = "replace", index = False)

    #Наша функция возвращает датафрейм ошибок произошедших в день Date
    return pd.read_sql("SELECT * FROM results", conn)

print(load_errors("2021-04-27"))

'''Замер памяти
Line #    Mem usage    Increment  Occurrences   Line Contents
=============================================================
     6     73.2 MiB     73.2 MiB           1   @profile
     7                                         def load_errors(Date):
     8                                             #Считываем данные и файлов в Dataframe pandas
     9    293.8 MiB    220.5 MiB           1       server_df = pd.read_csv("server.csv")
    10    509.8 MiB    216.0 MiB           1       client_df = pd.read_csv("client.csv")
    11
    12                                             #Приводим столбцы дат к типу datetime
    13    509.0 MiB     -0.8 MiB           1       server_df.timestamp = pd.to_datetime(server_df.timestamp, unit="s")
    14    508.0 MiB     -1.0 MiB           1       client_df.timestamp = pd.to_datetime(client_df.timestamp, unit="s")
    15
    16                                             #Загружаем таблицу cheaters
    17    508.3 MiB      0.3 MiB           1       conn = sqlite3.connect("cheaters.db")
    18    509.8 MiB      1.6 MiB           1       cheaters_df = pd.read_sql("SELECT * FROM cheaters", conn, parse_dates="ban_time")
    19
    20                                             #Объединяем таблицы
    21                                             #Поскольку в итоговой таблице должны быть заполнены поля server_timestamp и player_id то делаем inner join, хотя и потеряем часть данных
    22                                             #На самом деле немного странно, что при почти одинаковом колве записей(Server 66675, Client 66679) обе 
таблицы на одну половину состоят
    23                                             #из своих, а на другую из общих по Error_id записей
    24    516.2 MiB      6.4 MiB           1       joined_df = client_df.merge(server_df, on='error_id', how ='inner', suffixes=('_client', '_server'))   
    25
    26                                             #Выбираем данные за дату date
    27    516.2 MiB      0.0 MiB           1       Date1 = (datetime.strptime(Date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    28    516.2 MiB      0.0 MiB           1       query = "'"+Date+"'" + " <= timestamp_server <" + "'"+Date1+"'"
    29    516.3 MiB      0.1 MiB           1       joined_df = joined_df.query(query)
    30
    31                                             #Объединяем таблицу с читерами
    32    516.4 MiB      0.0 MiB           1       result_df = joined_df.merge(cheaters_df, on='player_id', how ='left')
    33    516.4 MiB      0.0 MiB           1       result_df.ban_time = result_df.ban_time.fillna(datetime.today())
    34
    35                                             #Удаляем часть читеров по условию
    36    516.4 MiB      0.0 MiB           1       result_df = result_df[result_df["ban_time"] + timedelta(days=1) > result_df["timestamp_server"]]       
    37
    38                                             #Создаем итоговую таблицу
    39    516.4 MiB      0.0 MiB           1       conn = sqlite3.connect("results.db")
    40    516.4 MiB      0.0 MiB           1       res_cols = ["timestamp", "player_id", "event_id", "error_id", "json_server", "json_client"]
    41    516.4 MiB      0.0 MiB           1       rename_dict = {"timestamp_server" : "timestamp", "description_server" : "json_server", "description_client" : "json_client"}
    42    516.4 MiB      0.0 MiB           1       result_df.rename(columns= rename_dict, inplace= True)
    43    516.6 MiB      0.2 MiB           1       result_df[res_cols].to_sql("results", conn, if_exists = "replace", index = False)
    44
    45                                             #Наша функция возвращает датафрейм ошибок произошедших в день Date
    46    516.6 MiB      0.0 MiB           1       return pd.read_sql("SELECT * FROM results", conn)
    '''