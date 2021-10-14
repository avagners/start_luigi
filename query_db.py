import sqlite3


def delete_record(date):
    try:
        sqlite_connection = sqlite3.connect('sqlite.db')
        cursor = sqlite_connection.cursor()
        print("Подключен к SQLite")

        sql_delete_query = f"""
        DELETE FROM nyc_trip_agg_data
        WHERE pickup_date {date};
        """
        cursor.execute(sql_delete_query)
        sqlite_connection.commit()
        print("Запись успешно удалена")
        cursor.close()

    except sqlite3.Error as error:
        print("Ошибка при работе с SQLite", error)
    finally:
        if sqlite_connection:
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


def select_data():
    sqlite_connection = sqlite3.connect('sqlite.db')
    cursor = sqlite_connection.cursor()
    print("Подключен к SQLite")
    for row in cursor.execute(
        """
        SELECT pickup_date, tip_amount, total_amount
        FROM nyc_trip_agg_data
        WHERE pickup_date > '2020-12-31'
        ORDER BY pickup_date;
        """
        ):
        print(row)
    print("Данные выведены")
    sqlite_connection.close()


# delete_record('> 2020-12-31')
select_data()
