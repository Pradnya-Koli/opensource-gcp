import psycopg2
import json
import logging
import mysql.connector
import pandas as pd
import re
from Create_BigQuery_Table import *
def mysql_ddl_extractor():
    with open("mysql_config.json") as config_file:
        data = json.load(config_file)
    open('Source_DDL/source_ddl.sql', 'w').close()
    database = data['details']['database']
    database_list=[]
    database_list.append(database)
    logger = logging.getLogger(__name__)
    try:
        conn = mysql.connector.connect(

            host=data['details']['host'],
            port=data['details']['port'],
            user=data['details']['username'],
            password=data['details']['password'],
            database=data['details']['database']
        )
        cur = conn.cursor()
        sql = """use {} """.format(database)
        cur.execute(sql)
        sql = """SHOW FULL TABLES IN {} WHERE TABLE_TYPE != 'VIEW';""".format(database)
        cur.execute(sql)
        result = cur.fetchall()
        frame1 = pd.DataFrame(result)
   #     print("frame", frame1)
        ddl = []


        viewddl = []
        view_Sql = "SHOW FULL TABLES IN {} WHERE TABLE_TYPE LIKE 'VIEW';".format(database)
        cur.execute(view_Sql)
        View_tables = cur.fetchall()
        View_tables = pd.DataFrame(View_tables)


        create_bq_dataset(database_list)
        for ind in frame1.index:
            cur.execute("show create table " + frame1[0][ind])
            res = cur.fetchall()
            ddl.append(res)

            # print("ddl",ddl)
        for row in View_tables.index:
            cur.execute("show columns from {};".format(View_tables[0][row]))
            view_result = cur.fetchall()
            viewddl.append(view_result)


        for ind in ddl:
            text = ind
#            print(text)
            text = str(text)
            text = re.sub(r'[\[{}\]]', '', text, flags=re.IGNORECASE)
            # text = re.sub(r'\\n','', text, flags=re.IGNORECASE)
            text = re.sub(r'["``"]', '', text, flags=re.IGNORECASE)
            spilt_strig = text.split(' ', 1)[1]
            spilt_strig = spilt_strig.replace("\\n", "\n")
            spilt_strig = spilt_strig.replace("'", "")
            spilt_strig = spilt_strig.replace("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci)", " ").replace("ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=latin1) ;"," ")
            spilt_strig = spilt_strig.replace(
                "ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci)", " ")
            spilt_strig = spilt_strig.replace(" ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1) ; "," ")
            spilt_strig = spilt_strig.replace("ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=latin1)"," ").replace("ENGINE=InnoDB DEFAULT CHARSET=latin1)"," ").replace("ENGINE=InnoDB DEFAULT CHARSET=latin1)"," ")
           # print(spilt_strig)
            temp = spilt_strig.split(" ")
            mysqlddl = ""
            for i in range(len(temp)):
                if i == 2:
                    mysqlddl = mysqlddl + "{}.{} ".format(database, temp[i])
                    print(mysqlddl)

                else:
                    mysqlddl = mysqlddl + temp[i]+" "
#                    print(mysqlddl)
            print(mysqlddl)
            f = open("Source_DDL/source_ddl.sql", "a+")
            f.write(mysqlddl + ";" + "\n")
            f.close()
        for indview in viewddl:
            text = indview
            text = str(text)


            final_view = text.replace("[", "").replace("('", "").replace("',", "").replace("b'", "").replace("'NO",
                                                                                                             "").replace(
                "", "").replace("None,", "") \
                .replace("'')", "").replace("'", "").replace("]", "")

            getddl = ("CREATE TABLE {}{}{}({}); ".format(database, temp, View_tables[0][row], final_view))
            print(getddl, "\n")
            f = open("Source_DDL/source_ddl.sql", 'a+')
            f.write(text + ";" + "\n")
            f.close()
    except psycopg2.Error:
        logger.exception('Failed to open database connection.')
        print("failed")


mysql_ddl_extractor()
