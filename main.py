from __future__ import absolute_import

import argparse
import csv
import io
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta

import apache_beam as beam
import jaydebeapi
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


# from google.cloud import bigquery


class ProcessOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments
        # extraer argumentos de entrada.
        parser.add_value_provider_argument(
            '--proyect_flujo',
            help='proyect')
        parser.add_value_provider_argument(
            '--dataset_tabla_fuente',
            help='dataset_tabla_fuente')
        parser.add_value_provider_argument(
            '--name_tabla_fuente',
            help='name_tabla_fuente')
        parser.add_value_provider_argument(
            '--cadenaserver',
            help='cadenaserver')
        parser.add_value_provider_argument(
            '--userserver',
            help='userserver')
        parser.add_value_provider_argument(
            '--passserver',
            help='passserver')
        parser.add_value_provider_argument(
            '--bucketfiles',
            help='bucketfiles')


class SplitWords(beam.DoFn):
    def __init__(self, delimiter=','):
        self.delimiter = delimiter

    def process(self, text):
        for word in text.split(self.delimiter):
            yield word


def split_words(text):
    return text.split(',')


def generate_elements(elements):
    for element in elements:
        logging.info("Analizando elemento" + str(element))
        yield element


class RegEntrada(beam.DoFn):
    def process(self, input, arg):
        logging.info("------------INICIA_PROCESO-------" + str(input))
        yield input


class RegSalida(beam.DoFn):
    def process(self, input):
        logging.info("-----------TERMINA_PROCESO-------" + str(input))


class FuncContar(beam.DoFn):
    def process(self, input):
        for i in range(11):
            print(i)
        time.sleep(5)
        yield input


class GenerateVals(beam.DoFn):
    def process(self, input):
        lstdict = [
            {"name": "Klaus", "age": 32},
            {"name": "Elijah", "age": 33},
            {"name": "Kol", "age": 28},
            {"name": "Stefan", "age": 8}
        ]
        for obj in lstdict:
            yield obj


class ProcessValUno(beam.DoFn):
    def process(self, input):
        logging.info("-----------INI_PROCESO_1-------" + str(input['name']))
        # for i in range(10):
        #    print(i)
        # time.sleep(5)
        logging.info("-----------FIN_PROCESO_1-------" + str(input['name']))
        return list("1")


class ProcessValDos(beam.DoFn):
    def process(self, input):
        logging.info("-----------INI_PROCESO_2-------" + str(input['name']))
        # for i in range(10):
        #    print(i)
        # time.sleep(5)
        logging.info("-----------FIN_PROCESO_2-------" + str(input['name']))
        return list("1")


def trasform_fecha(str_type, val_read):
    if val_read is None:
        return None

    if str_type == "STRING":
        val_type = str(val_read)
    if str_type == "TIMESTAMP":
        p = re.compile('([0-9]*-[0-9]*-[0-9]*)T([0-9]*:[0-9]*:[0-9]*.[0-9]*)')
        m = p.match(val_read)
        fec_str = m.group(1) + " " + m.group(2)
        val_type = datetime.strptime(fec_str, '%Y-%m-%d %H:%M:%S.%f')
    if str_type == "DATE":
        p = re.compile('([0-9]*-[0-9]*-[0-9]*)([a-zA-Z0-9\s":{},\]\[.-]*)')
        m = p.match(val_read)
        fec_str = m.group(1)
        val_type = datetime.strptime(fec_str, '%Y-%m-%d')  # .strftime("%Y-%m-%d")

    return val_type


def save_logs_bq(data, known_args):
    from google.cloud import bigquery

    logging.info("Entra a crear log")
    logging.info(data["LOGS"])
    client_bq = bigquery.Client()

    # query = element["Query_ingesta"] % (element['Dataset_stg'] + ".STG_" + element["Tabla_destino"])
    # query = "INSERT `pe-udps-datalake-prd.Logs.Logs_flujo` ( Pipeline, Query_extraccion, Query_ingesta, Delta_conteo, Id_empresa, Empresa, Tabla_destino, Dataset_destino, Tiempo_extraccion, Tipo_extraccion, Pivote, Valor_pivote_pre, Valor_pivote_fin, Estado, Error, Fecha) \
    query = "INSERT `pe-udps-datalake-prd.Logs.Logs_flujo` ( Pipeline, Query_extraccion, Query_ingesta, Delta_conteo, Id_empresa, Empresa, Tabla_destino, Dataset_destino, Tiempo_extraccion, Tipo_extraccion, Pivote, Valor_pivote_pre, Valor_pivote_fin, Estado, Error, Fecha) \
    VALUES('" + data["LOGS"]["Pipeline"] + "','" + data["LOGS"]["Query_extraccion"] + "','" + data["LOGS"]["Query_ingesta"] + "','" + \
            data["LOGS"]["Delta_conteo"] + "','" \
            "" + data["LOGS"]["Id_empresa"] + "','" + data["LOGS"]["Empresa"] + "','" + \
            data["LOGS"]["Tabla_destino"] + "','" + data["LOGS"]["Dataset_destino"] + "','" + data["LOGS"]["Tiempo_extraccion"] + "" \
                                                                                                         "','" + \
            data["LOGS"]["Tipo_extraccion"] + "','" + data["LOGS"]["Pivote"] + "','" + data["LOGS"]["Valor_pivote_pre"] + "','" + \
            data["LOGS"]["Valor_pivote_fin"] + "','" + data["LOGS"]["Estado"] + "','" + data["LOGS"]["Error"] +"','" + str(data["LOGS"]["Fecha"]) +  "')"

    logging.info(query)


    query_job = client_bq.query(query)  # Make an API request.
    if query_job.errors is not None:
        logging.error("info_process:error_actualizada_tabla___" + str(query_job.errors))
        # generate_log(beam_name, "OK", element, known_args, str(query_job.errors))
    else:
        logging.info("Info_process:_Acatualizada_tabla_logs")
    # data["LOGS"]
    return


class setenv(beam.DoFn):
    def process(self, data, known_args):
        """
        Primer flujo, se instala java y se descarga los archivo jre para conexion jdbc

        :param context
        :param known_args: bucket_files, bucket en donde se encuetran los archivos.
        :return: Variable creada para guardar los datos de auditoria.
        """

        # bucket_files = "dataflow_archivos"
        bucket_files = known_args.bucketfiles.get()

        if os.system('gsutil cp gs://' + bucket_files + '/mssql-jdbc-9.2.1.jre8.jar /tmp/' + '&&' +
                     'gsutil cp -r gs://' + bucket_files + '/jdk-8u181-linux-x64.tar.gz /tmp/') != 0:
            logging.info("info_process_error_al_copiar_del_bucket")
        else:
            logging.info('info_process_Jar_copiado')
            logging.info('info_process_Java_Lib_copiadas')

        if os.system(
                'mkdir -p /usr/lib/jvm  && tar zxvf /tmp/jdk-8u181-linux-x64.tar.gz -C /usr/lib/jvm  && update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/jdk1.8.0_181/bin/java" 1 && update-alternatives --config java') != 0:
            logging.info("info_process_error_al_instal_java")
        else:
            logging.info('info_process_enviornment_Variable_set.')

        return [data]


class ParametricReading(beam.DoFn):
    def process(self, init, known_args):

        from google.cloud import bigquery

        try:
            client_bq = bigquery.Client()

            query = "SELECT * FROM `"+known_args.proyect_flujo.get()+"."+known_args.dataset_tabla_fuente.get()+"."+ known_args.name_tabla_fuente.get()\
                    +"` where Activo = 'si'"

            query_job = client_bq.query(query)  # Make an API request.
            #query_job = client_bq.query(query).to_dataframe()
            #uery_job = client_bq.query(query).to_dataframe()
            results = query_job.result()
            #df = query_job.to_dataframe()
            #json_obj = df.to_json(orient='records')
            #records = [dict(row) for row in results]
            #json_obj = json.dumps(str(records))

            day = datetime.today().weekday()
            num_day = datetime.now().strftime('%d')

            for row in results:

                process_run = 0


                #data = {}
                data = dict(row)

                logging.info("info_process_evaluando_proceso_" + data["Tabla_destino"] + " Empresa " + data["Empresa"])

                data["Tiempo_extraccion"] = data["Tiempo_extraccion"].replace(" ", "")
                # se corre o no segun tiempo de extraccion diario semanal mensual
                if data["Tiempo_extraccion"] == "diario" and data["Activo"] == "si":
                    logging.info("info_process_extraccion_diaria_" + data["Tabla_destino"] + " Empresa " + data["Empresa"])
                    process_run = 1
                elif data["Tiempo_extraccion"] == "semanal" and day == 6  and data["Activo"] == "si":
                    logging.info("info_process_extraccion_semanal_" + data["Tabla_destino"] + " Empresa " + data["Empresa"])
                    procss_run = 1
                elif data["Tiempo_extraccion"] == "mensual" and num_day == "01"  and data["Activo"] == "si":
                    logging.info("info_process_extraccion_mensual_" + data["Tabla_destino"] + " Empresa " + data["Empresa"])
                    process_run = 1
                else:
                    logging.info(
                        "info_process_no_se_ejecuta_por_tiempo_de_extraccion_o_bandera" + data["Tabla_destino"] + "_" +
                        data["Tiempo_extraccion"] + " Empresa " + data["Empresa"])

                if process_run == 1:
                    # data["LOGS"] = process_logs
                    data["LOGS"] = {}
                    data["LOGS"]["Pipeline"] = "ParametricReading"
                    data["LOGS"]["Query_extraccion"] = ""
                    data["LOGS"]["Query_ingesta"] = ""
                    data["LOGS"]["Delta_conteo"] = "-1"
                    data["LOGS"]["Partes_conteo"] = "-1"
                    data["LOGS"]["Id_empresa"] = data["Id_empresa"]
                    data["LOGS"]["Empresa"] = data["Empresa"]
                    data["LOGS"]["Tabla_destino"] = data["Tabla_destino"]
                    data["LOGS"]["Dataset_destino"] = data["Dataset_destino"]
                    data["LOGS"]["Tiempo_extraccion"] = data["Tiempo_extraccion"]
                    data["LOGS"]["Tipo_extraccion"] = data["Tipo_extraccion"]
                    data["LOGS"]["Pivote"] = data["Pivote"]
                    data["LOGS"]["Valor_pivote_pre"] = data["Valor_pivote"]
                    data["LOGS"]["Valor_pivote_fin"] = ""
                    data["LOGS"]["Estado"] = "DONE"
                    data["LOGS"]["Error"] = ""
                    data["LOGS"]["Fecha"] = datetime.now()

                    data["part"] = ""

                    yield data
                    del data

        except Exception as e:
            logging.info("info_process_error_al_leer_tabla_fuente")
            logging.error(e)
            data["LOGS"]["Error"] = "Error al leer tabla fuente desde fuente"
            data["LOGS"]["Estado"] = "ERROR"
            save_logs_bq(data, known_args)

class ExtractDeltaData(beam.DoFn):
    def process(self, data, known_args):
        """
        Tercer flujo, en base a los datos leidos de la tabla fuente se extrae la informacion desde
        sql server, luego se generar una tabla delta temporal con los datos extraidos.

        :param data: Datos de tabla de entrada leida desde tabla fuente
        :param known_args: Datos de conexion para bases de datos
        :return: Datos de la tabla leida de tabla fuenta con nuevos datos de auditoria.
        """
        from google.cloud import bigquery
        try:
            # logging.info("info_process_inicia_extraccion_delta_" + data["nombre_tabla"])
            logging.info(
                "INFO_PROCESS:_STG_TABLA" + data["Tabla_destino"] + " Empresa " + data["Empresa"])

            data["LOGS"]["Pipeline"] = "ExtractDeltaData"

            client_bq = bigquery.Client()
            query = "TRUNCATE TABLE `" + known_args.proyect_flujo.get() + ".STG.STG_" + data["Tabla_destino"] +"_"+data["Empresa"] + "`"
            logging.info(query)
            query_job = client_bq.query(query)  # Make an API request.
            if query_job.errors is not None:
                logging.error("info_process:error_limpiando tabla STG____" + str(query_job.errors))
                # generate_log(beam_name, "OK", element, known_args, str(query_job.errors))

            # LEER BASE DE DATOS SQL SERVER
            # Conector SQL server
            #data["database"] = "Abohl_Prueba"

            # logging.info("Cadena: " + known_args.cadenaserver.get())
            # logging.info("Server: " + known_args.userserver.get())
            # logging.info("Pass: " + known_args.passserver.get())
            # logging.info("Base: " + data["database"])

            conn = jaydebeapi.connect("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                                      # f"""jdbc:sqlserver://10.128.0.4\ENCUENTROS:1434;
                                      f"""%s;
                                            databaseName=%s""" % (known_args.cadenaserver.get(), data["Base_de_datos"]),
                                      [known_args.userserver.get(), known_args.passserver.get()],
                                      ["tmp/postgresql-42.2.19.jar", "tmp/mssql-jdbc-9.2.1.jre8.jar"])

            #conn = jaydebeapi.connect("com.microsoft.sqlserver.jdbc.SQLServerDriver",
            #                          # f"""jdbc:sqlserver://10.128.0.4\ENCUENTROS:1434;
            #                          f"""%s;
            #                                            databaseName=%s""" % (
            #                          known_args.cadenaserver, data["Base_de_datos"]),
            #                          [known_args.userserver, known_args.passserver],
            #                          ["tmp/postgresql-42.2.19.jar", "tmp/mssql-jdbc-9.2.1.jre8.jar"])
            curs_sql = conn.cursor()

            # Realiza conteo de base de datos entrada
            logging.info("info_process_inicia_verificar_pivote")

            # Fecha de Carga
            fecha_time = data["LOGS"]["Fecha"]
            fecha_date = datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")

            if data["Pivote"] is not None and data["Pivote"] != "":
                if data["Pivote"] == "fecha":
                    # Se traen Datos del dia anterior hasta el día anterior
                    fecpiv = datetime.now() - timedelta(1)

                    # if data["Valor_pivote"] is not None and data["Valor_pivote"] != "":
                    query = data['Query_extraccion'] % (
                    data['Id_empresa'], data['Empresa'], data['Valor_pivote'], fecpiv.strftime("%Y-%m-%d"))
                    data["Valor_pivote"] = fecpiv.strftime("%Y-%m-%d")
            else:
                if data['Tipo_de_tabla_origen'] == "F":
                    query = data['Query_extraccion'] % (data['Id_empresa'], data['Empresa'])
                else:
                    query = data['Query_extraccion']

            data["LOGS"]["Query_extraccion"] = re.sub(r"^a-zA-Z0-9 _*().-]","",query)
            data["LOGS"]["Query_extraccion"] = data["LOGS"]["Query_extraccion"].replace("'", " ")
            # query = "select TOP 1 * from dbo.fFB_BI_Almacenes(0,'')"
            # query = "Select TOP 2000 * from dbo.fFB_BI_Ventas(0,'BOHL','','2022-05-16')"

            logging.info("info_process_obteniendo_registros query " + query)
            # logging.info(val)

            count_part = 0
            count_data = 0
            """
            df = pd.DataFrame()
            nombres = ['Juan', 'Laura', 'Pepe']
            edades = ['42', '40', '37']
            edades = [42, 40, 37]

            df['Empresa'] = nombres
            df['PKID'] = edades
            listprueba = []
            listprueba.append(df)

            for data_delta in listprueba:
            """

            keys_obj = data['objeto_respuesta_mapeo'].split(sep=',')

            for data_delta in pd.read_sql_query(query, conn, chunksize=100000):
                count_data += len(data_delta)
                count_part += 1
                data["part"] = "_part_" + str(count_part)

                logging.info(data_delta)

                logging.info("Tipos de Datos")
                for column in data_delta:

                    #logging.info(column)

                    p = re.compile(
                        '([a-zA-Z0-9\s":{},\]\[]*"name":\s*"' + column + '",\s*"type":\s*")([A-Z]+)')
                    m = p.match(data["Esquema_str"])
                    str_type = m.group(2)
                    # logging.info("Tipos de Datos BQ")
                    # logging.info('Tipo de la dato Esquema: ', str_type)
                    if str_type == "NUMERIC" or str_type == "INTEGER" or str_type == "FLOAT":
                        data_delta[column] = data_delta[column].apply(pd.to_numeric)
                    if str_type == "STRING":
                        data_delta[column] = data_delta[column].astype(str)
                    if str_type == "DATE":
                        data_delta[column] = pd.to_datetime(data_delta[column], format="%Y-%m-%d")
                    if str_type == "TIMESTAMP":
                        data_delta[column] = pd.to_datetime(data_delta[column], format="%Y-%m-%d, %H:%M:%S")

                data_delta = data_delta.assign(EMPRESAID_BQ=int(data['Id_empresa']))
                data_delta = data_delta.assign(FECHA_TIME_BQ=fecha_time)
                data_delta = data_delta.assign(FECHA_DATE_BQ=fecha_date)

                # logging.info(retval)

                data_dict = {}
                # data_dict["df"] = df_result
                df_result = pd.DataFrame(data_delta, columns=keys_obj)
                data_dict["df"] = df_result
                data_dict["conf"] = data
                listfinto = []
                listfinto.append(data_dict)

                yield listfinto
                logging.info("INFO_PROCESS:_ENVIO BLOQUE DE DATOS")
                # del retval
                del data_dict
                del listfinto

            data["LOGS"]["Delta_conteo"] = str(count_data)
            data["LOGS"]["Partes_conteo"] = str(count_part)
            logging.info("ENTREGA CONTEO " + data["LOGS"]["Delta_conteo"])
            logging.info("PARTES CONTEO " + data["LOGS"]["Partes_conteo"])

            if count_data > 0:
                listfinto = []
                listfinto.append(data)
                yield listfinto

            del count_data
            del count_part
            del data


        except Exception as e:
            logging.info("info_process_error_al_extraer_datos_delta")
            logging.error(e)
            data["LOGS"]["Error"] = "Error al extraer los datos desde fuente"
            data["LOGS"]["Estado"] = "ERROR"
            save_logs_bq(data, known_args)


class save_to_bigquery(beam.DoFn):
    def process(self, element, known_args):

        from google.cloud import bigquery

        try:
            element = element[0]
            logging.info(element)

            if "df" in element:
                element['conf']["LOGS"]["Pipeline"] = "Part STG"

                logging.info("info_process:_Entra a Crear Staging"+ element['conf']["Tabla_destino"]+" Empresa "+element['conf']["Empresa"]+ " parte "+element['conf']["part"])

                # ----------------------------------------------------
                """
                retval = {}
                data_dict = {}
                # data_dict["df"] = df_result
                data_dict["df"] = retval
                data_dict["conf"] = element
                listfinto = []
                listfinto.append(data_dict)
                element = listfinto
                """
                # -------------------------------------------------------
                client_bq = bigquery.Client()

                # table_id = '{flow_proyect}:{dataset}.delta_{datatable}'.format(flow_proyect = known_args.proyect_flujo.get(),dataset = element['conf']['Dataset_destino'],datatable = element['conf']["Tabla_destino"])
                table_id = '{dataset}.STG_{datatable}_{cliente}{part}'.format(dataset=element['conf']['Dataset_stg'],
                                                                    datatable=element['conf']["Tabla_destino"],
                                                                    cliente = element['conf']["Empresa"],
                                                                    part=element['conf']["part"])

                sal_json = json.loads(element['conf']["Esquema_str"])

                logging.info("info_process_configuracion_terminada")
                # test_esquema = '[{"mode": "NULLABLE", "name": "IDoportunidad", "type": "STRING"},{"mode": "NULLABLE", "name": "TipoPreventa", "type": "STRING"}, {"mode": "NULLABLE", "name": "EstadodelaPreventa", "type": "STRING"},{"mode": "NULLABLE", "name": "TipodeSolucion", "type": "STRING"}, {"mode": "NULLABLE", "name": "Comentarios", "type": "STRING"}, {"mode": "NULLABLE", "name": "FechaVencimiento", "type": "TIMESTAMP"}, {"mode": "NULLABLE", "name": "Producto", "type": "STRING"}, {"mode": "NULLABLE", "name": "Oportunidad", "type": "STRING"}, {"mode": "NULLABLE", "name": "NombredePreventa", "type": "STRING"}, {"mode": "NULLABLE", "name": "PaisCuenta", "type": "STRING"}, {"mode": "NULLABLE", "name": "TiempoTranscurrido", "type": "STRING"}, {"mode": "NULLABLE", "name": "RecursoResponsable", "type": "STRING"}, {"mode": "NULLABLE", "name": "FechadeCreacion", "type": "TIMESTAMP"}, {"mode": "NULLABLE", "name": "FechadeActualizacion", "type": "TIMESTAMP"}, {"mode": "NULLABLE", "name": "UsuarioquienActualizo", "type": "STRING"}, {"mode": "NULLABLE", "name": "FECHA_TIME_BQ", "type": "TIMESTAMP"}, {"mode": "NULLABLE", "name": "FECHA_DATE_BQ", "type": "DATE"}]'

                # creacion de job para tabla
                job_config = bigquery.LoadJobConfig(
                    schema=sal_json,
                    # schema=table_list,
                    write_disposition="WRITE_TRUNCATE",
                )

                job = client_bq.load_table_from_dataframe(element['df'], table_id,
                                                          job_config=job_config)  # Make an API request.
                # job = client_bq.load_table_from_json(element['df'], table_id, job_config=job_config)  # Make an API request.

                job.result()  # Wait for the job to complete.

                table = client_bq.get_table(table_id)  # Make an API request.
                logging.info("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))
                # element['conf']['conteo_final'] = str(table.num_rows)
                yield element
                del element
            else:
                logging.info("FIN DE LA ENTREGA DE PARTES STAGING")
                logging.info("ENTREGA CONTEO " + element["LOGS"]["Delta_conteo"]+" Empresa "+element["Empresa"])
                yield element
                del element

        except Exception as e:
            logging.info("INFO_PROCESS:_Error creando STG delta")
            logging.error(e)
            element["LOGS"]["Error"] = "Error al crear parte tabla STG"
            save_logs_bq(element, known_args)
            return {}


class create_stg(beam.DoFn):
    def process(self, element, known_args):

        from google.cloud import bigquery
        try:
            if "df" in element:
                logging.info("INFO_PROCESS:_STG_TABLA" + element['conf']["Tabla_destino"]+" Empresa "+element['conf']["Empresa"])
                element['conf']["LOGS"]["Pipeline"] = "create_stg"
                client_bq = bigquery.Client()
                # query = "INSERT `pe-udps-datalake-prd.BI.ODS_Almacen` SELECT * FROM `pe-udps-datalake-prd.STG.STG_Almacen`" % (
                #            element['Dataset_stg'] + ".STG_" + element["Tabla_destino"] + element["part"])
                query = "INSERT `" + known_args.proyect_flujo.get() + "." + element['conf']['Dataset_stg'] + ".STG_" + \
                        element['conf'][
                            "Tabla_destino"] +"_"+element['conf']["Empresa"]+ "` (SELECT * FROM `" + known_args.proyect_flujo.get() + "." + \
                        element['conf'][
                            'Dataset_stg'] + ".STG_" + element['conf']["Tabla_destino"] + "_"+element['conf']['Empresa']+element['conf']["part"] + "`)"
                logging.info(query)
                query_job = client_bq.query(query)  # Make an API request.
                results = query_job.result()

                if query_job.errors is not None:
                    logging.error("info_process:error_actualizada_creando_tabla_STG_" + str(query_job.errors))
                    element["LOGS"]["Error"] = "Error al ejecutar query al crear tabla STG"
                    save_logs_bq(element, known_args)
                else:
                    logging.info(
                        "info_process:actualizada_tabla_STG_" + element['conf']["Tabla_destino"] + element['conf'][
                            "part"])

                    table_id = '{dataset}.STG_{datatable}_{cliente}{part}'.format(
                        dataset=element['conf']['Dataset_stg'],
                        datatable=element['conf']["Tabla_destino"],
                        cliente=element['conf']["Empresa"],
                        part=element['conf']["part"])
                    #TODO QUTIR PARA PRUEBAS
                    client_bq.delete_table(table_id, not_found_ok=True)  # Make an API request.
                    del element
            else:
                logging.info("CREADA STAGING " + element["Tabla_destino"])
                yield element
                del element

        except Exception as e:
            logging.info("INFO_PROCESS:_Error creando STG")
            logging.error(e)
            element['conf']["LOGS"]["Error"] = "Error al crear tabla STG"
            element['conf']["LOGS"]["Estado"] = "ERROR"
            save_logs_bq(element, known_args)
            return {}


class save_to_production(beam.DoFn):
    def process(self, element, known_args):

        from google.cloud import bigquery
        try:

            element["LOGS"]["Pipeline"] = "Save ODS"

            beam_name = "Guardar"

            logging.info("INFO_PROCESS:_ODS_TABLA" + element["Tabla_destino"]+" Empresa "+element["Empresa"])
            logging.info("INFO_PROCESS:_PARTE" + element["part"])
            client_bq = bigquery.Client()
            # conteo = element["LOGS"]["Delta_conteo"]
            conteo_regist = 0
            ciclo_espera = 0
            cont_correct = 0
            while cont_correct == 0:
                query = "SELECT count(*) FROM `" + known_args.proyect_flujo.get() + "." + element[
                    'Dataset_stg'] + ".STG_" + element["Tabla_destino"] +"_"+element["Empresa"]+ "`"
                logging.info(query)
                query_job = client_bq.query(query)  # Make an API request.
                for row in query_job:
                    # Row values can be accessed by field name or index.
                    conteo_regist = row._xxx_values[0]
                if conteo_regist == int(element["LOGS"]["Delta_conteo"]):
                    cont_correct = 1
                else:
                    logging.info("Info_process:_Entra a esperar q terminen otros jobs")
                    time.sleep(10)  # espera en segundos
                    ciclo_espera += 1

                if ciclo_espera >= 12:
                    logging.error("INFO_PROCESS:No se creo correctamente staging para " + element["Tabla_destino"])
                    element["LOGS"]["Estado"] = "ERROR"
                    element["LOGS"]["Error"] = "No se creo correctamente staging para " + element["Tabla_destino"]
                    save_logs_bq(element, known_args)
                    return

            # query = "SELECT * FROM " + known_args.proyect_flujo.get() + "." + known_args.dataset_tabla_fuente.get() + "." + known_args.name_tabla_fuente.get() + " where Activo = 'si'"

            query = element["Query_ingesta"] % (element['Dataset_stg'] + ".STG_" + element["Tabla_destino"]+"_"+ element["Empresa"])
            logging.info(query)
            element["LOGS"]["Query_ingesta"] = re.sub(r"^a-zA-Z0-9 _*().-]","",query)
            element["LOGS"]["Query_ingesta"] = element["LOGS"]["Query_ingesta"].replace("'", " ")

            query_job = client_bq.query(query)  # Make an API request.
            if query_job.errors is not None:
                logging.error("info_process:error_actualizada_tabla___" + str(query_job.errors))
                element["LOGS"]["Error"] = "No se actualizo tabla fuente"
            else:
                logging.info("info_process:actualizada_tabla_PRD___")
                # query = "SELECT COUNT(*) FROM `" + known_args.proyect_flujo.get() + "." + element[
                #    'Dataset_destino'] + "." + element[
                #            "Tabla_destino"] + "`"
                # query_job = client_bq.query(query)
                # for row in query_job:
                #    element['conteo_final'] = str(row._xxx_values[0])
                # logging.info("info_process:_tabla_PRD___Conteo Final__"+element['conteo_final'] )

                query = "UPDATE `" + known_args.proyect_flujo.get() + "." + known_args.dataset_tabla_fuente.get() + "." + \
                        known_args.name_tabla_fuente.get() + "` SET Ultima_fecha = '" + str(element["LOGS"]["Fecha"]) + "' , Valor_pivote = '" + element["Valor_pivote"] + "' WHERE Tabla_destino = '" + \
                        element["Tabla_destino"] + "' and Empresa = '"+ element["Empresa"] + "'"

                logging.info(query)
                query_job = client_bq.query(query)  # Make an API request.
                if query_job.errors is not None:
                    logging.error("info_process:error_actualizada_tabla___" + str(query_job.errors))
                    element["LOGS"]["Error"] = "No se actualizo tabla fuente"
                else:
                    logging.info("info_process:actualizada_Tabla_Fuente___")

            save_logs_bq(element, known_args)
            del element


        except Exception as e:
            logging.info("INFO_PROCESS:_Error creando ODS")
            logging.error(e)
            element["LOGS"]["Error"] = "Error al crear tabla ODS"
            element["LOGS"]["Estado"] = "ERROR"
            save_logs_bq(element, known_args)
            return {}


class get_api_data(beam.DoFn):
    def __init__(self):
        logging.info("fetching api data")

    def process(self, element, known_args):
        import requests

        total_read = 0
        res_status = 1
        paso = "inicio"
        element["registros_delta"] = "0"
        element["conteo_final"] = "-"

        beam_name = "Get API data"

        try:
            if element['Activo'] != "si":
                logging.info("info_process:_No-se_procede_por_valor_en_campo_Activo-------%s", element['Activo'])
                return
            logging.info("info_process:_entra_a_procesar_a_tabla_destino_" + element["Tabla_destino"])

            url = element['Url_oauth']
            myobj = {'grant_type': "password",
                     'client_id': known_args.client_id.get(),
                     'client_secret': known_args.client_secret.get(),
                     'username': known_args.username.get(),
                     'password': known_args.password.get(),
                     }

            auth = requests.post(url, data=myobj).json()
            auth_access = auth['access_token']
            paso = "obtenido_token"

            bearer_token = auth_access
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + bearer_token,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
            next_records_url = ""
            keys_obj = element['objeto_respuesta_mapeo'].split(sep=',')
            field_map = element['Mapeo_campos_tabla'].split(sep=',')

            if len(keys_obj) != len(field_map):
                logging.error("info_process:_longitud_llaves_y_mapeo_no_corresponden_en_tabla_fuente")

            # datos = {"TABLEName": "TestTable"}

            fecha_time = datetime.now()
            # fecha_date = datetime.now().strftime("%Y-%m-%d")
            fecha_date = datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")

            list_result = []

            # df_marks = pd.DataFrame()

            # Preguntar por el maximo pivote
            query_max_date = "Select max(CreatedDate) from " + element["Tabla_origen"]
            query_url = element[
                            'Url_base'] + "/services/data/v54.0/query/?q=" + query_max_date  # +" limit 10"#"&Bearer Token="+auth_access+"&Content_Type=application/json"
            response = requests.get(query_url, headers=headers)
            paso = "obtenido_pivote"
            api_max_pivote = json.loads(response.text)['records'][0]['expr0']

            p = re.compile('([0-9]*-[0-9]*-[0-9]*)T([0-9]*:[0-9]*:[0-9]*)')
            m = p.match(api_max_pivote)
            api_max_pivote = m.group(1) + "T" + m.group(2) + "Z"

            element["api_max_pivote"] = api_max_pivote

            if element["Valor_pivote"] is not None:
                if api_max_pivote == element["Valor_pivote"]:
                    logging.info("info_process:_datos_actualizados_-------%s", element["Valor_pivote"])
                    generate_log(beam_name, "OK", element, known_args, "Sin cambios en los datos")
                    return
                else:
                    query_pivote = " where  CreatedDate > " + element[
                        "Valor_pivote"]  # and o.datefield__c < 2011-12-31"
            else:
                query_pivote = ""

            query_api = element['Query_extraccion']

            # prueba bulk
            # /services/data/vXX.X/jobs/query
            query_url = element[
                            'Url_base'] + "/services/data/v54.0/jobs/query"  # /?q=" + query_api + query_pivote  # +" limit 10"#"&Bearer Token="+auth_access+"&Content_Type=application/json"

            myobj = {
                "operation": "query",
                "query": query_api + query_pivote
                # "query": "Select Name,Opportunity.Id,Opportunity.Name,XER_Plazo_Producto_Meses__c,XER_Familia_Producto__c,Quantity,SP2_DescuentoFabricante__c,SP2_DescuentoXertica__c,UnitPrice,TotalPrice from OpportunityLineItem"
            }

            # query_url = element['Url_base'] + "/services/data/v54.0/query/?q=" + query_api + query_pivote#   +" limit 10"#"&Bearer Token="+auth_access+"&Content_Type=application/json"
            paso = "inicia_lectura_datos"
            while next_records_url is not None:
                logging.info("info_process:_request_query-------%s", query_url)

                response = requests.post(query_url, json=myobj, headers=headers)
                if "id" in json.loads(response.text):
                    id_job = json.loads(response.text)['id']
                else:
                    logging.error("info_process:_No_se_pudo_generar_el_trabajo")
                    logging.error(response.text)
                    generate_log(beam_name, "ERROR", element, known_args, "Error al generar el trabajo")
                    return

                respon_state = "InProgress"
                while respon_state == "UploadComplete" or respon_state == "InProgress":
                    query_url = element['Url_base'] + "/services/data/v54.0/jobs/query/" + id_job
                    response = requests.get(query_url, headers=headers)
                    respon_state = json.loads(response.text)["state"]
                    time.sleep(1)

                if json.loads(response.text)["state"] != 'JobComplete':
                    # guardar log
                    res_status = 0;
                    # return
                else:
                    query_url = element['Url_base'] + "/services/data/v54.0/jobs/query/" + id_job + "/results"
                    response = requests.get(query_url, headers=headers)

                    # format response csv
                    response_csv = 1
                    reader = csv.DictReader(io.StringIO(response.text))
                    json_data = json.dumps(list(reader))
                    records = json.loads(json_data)

                    # query_url = "https://xerticalabs--uat.my.salesforce.com/services/data/v54.0/query/?q=Select Id,Name,XER_Descripcion_oportunidad__c,Campaign.Id,Campaign.Name,CloseDate,Amount,Account.Id,Account.Name,Owner.Name,StageName,LeadSource,XER_Forecast__c,MVP_BillingID__c,SOP_Pendiente_por_Facturar__c,Loss_Reason__c,XER_Proceso__c,Probability,CreatedDate,LastModifiedDate,LastModifiedBy.Name from Opportunity"
                    # response = requests.get(query_url, headers=headers)
                    total_read += len(records)
                    logging.info("info_process:_request_numero_registros-------%d", total_read)

                if res_status == 1:
                    for row in records:
                        x = 0
                        retval = dict.fromkeys(field_map, "")

                        for key in keys_obj:
                            # p = re.compile('("name": "' + field_map[x] + '", "type": ")([A-Z]+)')
                            p = re.compile(
                                '([a-zA-Z0-9\s":{},\]\[]*"name":\s*"' + field_map[x] + '",\s*"type":\s*")([A-Z]+)')
                            m = p.match(element["Esquema_str"])
                            str_type = m.group(2)
                            # print("tipo de dato leido"+m.group(2))
                            if key.find(".") < 0 or response_csv == 1:
                                # retval[field_map[x]] = row[key]
                                if row[key] != "" and row[key] is not None:
                                    val_type = trasform_fecha(str_type, row[key])
                                else:
                                    val_type = None
                            else:
                                long_key = len(key.split(sep='.'))
                                count = 0
                                if row[key.split(sep='.')[0]] is not None:
                                    temp_dict = row[key.split(sep='.')[0]]
                                    for key_name in key.split(sep='.'):

                                        if count == 0:
                                            count += 1
                                            continue
                                        if temp_dict[key_name] is not None:
                                            temp_dict = temp_dict[key_name]
                                        else:
                                            temp_dict = None
                                        count += 1
                                else:
                                    temp_dict = None

                                if temp_dict is not None:
                                    val_type = trasform_fecha(str_type, temp_dict)
                                else:
                                    val_type = None

                                """
                                    if row[key.split(sep='.')[0]] is not None:
                                        #retval[field_map[x]] = row[key.split(sep='.')[0]][key.split(sep='.')[1]]
                                        val_type = trasform_fecha(str_type,row[key.split(sep='.')[0]][key.split(sep='.')[1]])
                                    else:
                                        #retval[field_map[x]] = None
                                        val_type = None
                                """
                            retval[field_map[x]] = val_type
                            x += 1

                        retval["FECHA_DATE_BQ"] = fecha_date
                        retval["FECHA_TIME_BQ"] = fecha_time

                        # yield retval
                        list_result.append(retval)
                        del retval
                        # df_marks = df_marks.append(retval, ignore_index=True)

                    if 'nextRecordsUrl' in response:
                        logging.info("info_process:_Found_Next_Url_obtain-------")
                        next_records_url = response.json()['nextRecordsUrl']
                        # next_records_url = "/services/data/v54.0/query/01gD0000002HU6KIAW-2000"
                        logging.info("info_process:_Next_Url_obtain-------" + next_records_url)
                        query_url = element['Url_base'] + next_records_url
                    else:
                        logging.info("info_process:_request_total_registros-------%d----", total_read)
                        element["registros_delta"] = str(total_read)
                        next_records_url = None
                else:
                    next_records_url = None
                    logging.error("info_process:_error_al_generar_request_error___%s",
                                  json.loads(response.text)["state"])
                    return

            # df_result = pd.DataFrame(list_result,columns=["NombredelProducto","IDdeOportunidad","Oportunidad","PlazoProducto","FamiliadeProducto","Cantidad","DescuentodeFabricante","DescuentodeXertica","PreciodeVenta","PrecioTotal","FECHA_DATE_BQ","FECHA_TIME_BQ"])
            # df_result = pd.DataFrame(list_result)
            field_map.append("FECHA_TIME_BQ")
            field_map.append("FECHA_DATE_BQ")
            df_result = pd.DataFrame(list_result, columns=field_map)
            # otro = pd.DataFrame([retval],columns=["NombredelProducto","IDdeOportunidad","Cantidad"])
            # otro2 = pd.DataFrame([retval], columns=["Cantidad","IDdeOportunidad","NombredelProducto"])
            data_dict = {}
            # data_dict["df"] = df_result
            data_dict["df"] = df_result
            data_dict["conf"] = element
            listfinto = []
            listfinto.append(data_dict)
            return listfinto
        except Exception as e:
            logging.error(e)
            generate_log(beam_name, "ERROR", element, known_args, str(e))
            return {}


"""
class save_to_bigquery(beam.DoFn):
    def process(self, element, known_args):

        try:

            beam_name = "Crear Delta"

            client_bq = bigquery.Client()

            # table_id = '{flow_proyect}:{dataset}.delta_{datatable}'.format(flow_proyect = known_args.proyect_flujo.get(),dataset = element['conf']['Dataset_destino'],datatable = element['conf']["Tabla_destino"])
            table_id = '{dataset}.delta_{datatable}'.format(dataset=element['conf']['Dataset_destino'],
                                                            datatable=element['conf']["Tabla_destino"])

            sal_json = json.loads(element['conf']["Esquema_str"])

            # test_esquema = '[{"mode": "NULLABLE", "name": "IDoportunidad", "type": "STRING"},{"mode": "NULLABLE", "name": "TipoPreventa", "type": "STRING"}, {"mode": "NULLABLE", "name": "EstadodelaPreventa", "type": "STRING"},{"mode": "NULLABLE", "name": "TipodeSolucion", "type": "STRING"}, {"mode": "NULLABLE", "name": "Comentarios", "type": "STRING"}, {"mode": "NULLABLE", "name": "FechaVencimiento", "type": "TIMESTAMP"}, {"mode": "NULLABLE", "name": "Producto", "type": "STRING"}, {"mode": "NULLABLE", "name": "Oportunidad", "type": "STRING"}, {"mode": "NULLABLE", "name": "NombredePreventa", "type": "STRING"}, {"mode": "NULLABLE", "name": "PaisCuenta", "type": "STRING"}, {"mode": "NULLABLE", "name": "TiempoTranscurrido", "type": "STRING"}, {"mode": "NULLABLE", "name": "RecursoResponsable", "type": "STRING"}, {"mode": "NULLABLE", "name": "FechadeCreacion", "type": "TIMESTAMP"}, {"mode": "NULLABLE", "name": "FechadeActualizacion", "type": "TIMESTAMP"}, {"mode": "NULLABLE", "name": "UsuarioquienActualizo", "type": "STRING"}, {"mode": "NULLABLE", "name": "FECHA_TIME_BQ", "type": "TIMESTAMP"}, {"mode": "NULLABLE", "name": "FECHA_DATE_BQ", "type": "DATE"}]'

            # creacion de job para tabla
            job_config = bigquery.LoadJobConfig(
                schema=sal_json,
                # schema=table_list,
                write_disposition="WRITE_TRUNCATE",
            )

            job = client_bq.load_table_from_dataframe(element['df'], table_id,
                                                      job_config=job_config)  # Make an API request.
            # job = client_bq.load_table_from_json(element['df'], table_id, job_config=job_config)  # Make an API request.

            job.result()  # Wait for the job to complete.

            table = client_bq.get_table(table_id)  # Make an API request.
            print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))
            #element['conf']['conteo_final'] = str(table.num_rows)
            return [element['conf']]

        except Exception as e:
            logging.error(e)
            generate_log(beam_name, "ERROR", element, known_args, str(e))
            return {}
"""


def generate_log(beam_name, status, element, known_args, log=""):
    from google.cloud import bigquery
    try:
        fecha_time = datetime.now()

        client_bq = bigquery.Client()
        query = "INSERT `" + known_args.proyect_flujo.get() + "." + known_args.dataset_tabla_fuente.get() + "." + "LOG_SALESFORCE" + "` VALUES('" + beam_name + "','" + status + "','" + str(
            fecha_time) + "','" + element["Tabla_destino"] + "','" + element["Componente"] + "','" + log + "','" + \
                element["registros_delta"] + "','" + str(element["Valor_pivote"]) + "','" + element[
                    "api_max_pivote"] + "','" + " " + "'" + ")"

        query_job = client_bq.query(query)  # Make an API request.
        if query_job.errors is not None:
            logging.error("info_process:error_actualizada_tabla__LOG___" + str(query_job.errors)
                          )
        else:
            return

    except Exception as e:
        logging.error(e)
        return {}


def Generate_esquema(element):
    from google.cloud import bigquery
    client = bigquery.Client()

    query = 'SELECT Tabla_destino,Esquema_str FROM `xertica-delivery-data-service.testSaleforce.tabla_fuente` where Tabla_destino = "' + \
            element.split(".")[1] + '" LIMIT 1'
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    logging.error("info_process:_consultando-esquema___%s", element.split(".")[1])
    for row in query_job:
        # Row values can be accessed by field name or index.
        tab_esquema = row["Esquema_str"]

    # hh = '{"fields": [{"mode": "NULLABLE", "name": "TipoPreventa", "type": "STRING"}, {"mode": "NULLABLE", "name": "TipodeSolucion", "type": "STRING"}, {"mode": "NULLABLE", "name": "Comentarios", "type": "STRING"}, {"mode": "NULLABLE", "name": "FechaVencimiento", "type": "STRING"}, {"mode": "NULLABLE", "name": "Producto", "type": "STRING"}, {"mode": "NULLABLE", "name": "Oportunidad", "type": "STRING"}, {"mode": "NULLABLE", "name": "NombredePreventa", "type": "STRING"}, {"mode": "NULLABLE", "name": "PaisCuenta", "type": "STRING"}, {"mode": "NULLABLE", "name": "TiempoTranscurrido", "type": "STRING"}, {"mode": "NULLABLE", "name": "RecursoResponsable", "type": "STRING"}, {"mode": "NULLABLE", "name": "FechadeCreacion", "type": "STRING"}, {"mode": "NULLABLE", "name": "FechadeActualizacion", "type": "STRING"}, {"mode": "NULLABLE", "name": "UsuarioquienActualizo", "type": "STRING"}, {"mode": "NULLABLE", "name": "FECHA_TIME_BQ", "type": "TIMESTAMP"}, {"mode": "NULLABLE", "name": "FECHA_DATE_BQ", "type": "DATE"}]}'
    sal_json = json.loads(tab_esquema)
    return sal_json


def Read_esquema(known_args):
    from google.cloud import bigquery
    client = bigquery.Client()
    esquema_dic = {}

    # query = 'SELECT Tabla_destino,Esquema_str FROM `xertica-delivery-data-service.testSaleforce.tabla_fuente` where Activo = "si"'
    query = "SELECT * FROM " + known_args.proyect_flujo.get() + "." + known_args.dataset_tabla_fuente.get() + "." + known_args.name_tabla_fuente.get() + " where Activo = 'si'"
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    logging.error("info_process:_consultando-esquemas___")
    for row in query_job:
        # Row values can be accessed by field name or index.
        esquema_dic[row["Tabla_destino"]] = row["Esquema_str"]
    # hh = '{"fields": [{"mode": "NULLABLE", "name": "TipoPreventa", "type": "STRING"}, {"mode": "NULLABLE", "name": "TipodeSolucion", "type": "STRING"}, {"mode": "NULLABLE", "name": "Comentarios", "type": "STRING"}, {"mode": "NULLABLE", "name": "FechaVencimiento", "type": "STRING"}, {"mode": "NULLABLE", "name": "Producto", "type": "STRING"}, {"mode": "NULLABLE", "name": "Oportunidad", "type": "STRING"}, {"mode": "NULLABLE", "name": "NombredePreventa", "type": "STRING"}, {"mode": "NULLABLE", "name": "PaisCuenta", "type": "STRING"}, {"mode": "NULLABLE", "name": "TiempoTranscurrido", "type": "STRING"}, {"mode": "NULLABLE", "name": "RecursoResponsable", "type": "STRING"}, {"mode": "NULLABLE", "name": "FechadeCreacion", "type": "STRING"}, {"mode": "NULLABLE", "name": "FechadeActualizacion", "type": "STRING"}, {"mode": "NULLABLE", "name": "UsuarioquienActualizo", "type": "STRING"}, {"mode": "NULLABLE", "name": "FECHA_TIME_BQ", "type": "TIMESTAMP"}, {"mode": "NULLABLE", "name": "FECHA_DATE_BQ", "type": "DATE"}]}'

    return esquema_dic


def Choise_esquema(table, esquema_dic):
    logging.info("info_process:_inicia_obtener_esquema_tabla_" + table)
    sal_json = None
    sq = esquema_dic[table.split(".")[1]]
    if sq is not None:
        sal_json = json.loads(sq)
    return sal_json


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    # known_args, pipeline_args = parser.parse_known_args(argv)
    # pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options = PipelineOptions()
    known_args = pipeline_options.view_as(ProcessOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # sq = Read_esquema(known_args)
    # val = [('', 58, 'Alvarez bohl - piura', 'PIURA', 'A')]

    #additional_bq_parameters = {
    #    'timePartitioning': {'type': 'DAY', 'field': 'FECHA_DATE_BQ'}
    #}

    #diccionario = {'nombre': 'Carlos', 'edad': 22.66, 'cursos': ['Python', 'Django', 'JavaScript']}
    #diccionario = [('Bohl', 24258, 55456, '2022-01-02', '2022-01-02', 0, 12, 0.0, 0.0, 0.0, 0.0, 1037, 530, 137083, 429, 4945, 215, 123200, 59, 0, 0, 'A', 'VE', 'F002-00000001', 58, 'A', 'A', '2022-01-01')]

    #df = pd.DataFrame()
    #nombres = ['Juan', 'Laura', 'Pepe']
    #edades = [42, 40, 37]

    #df['Nombre'] = nombres
    #df['Edad'] = edades

    #df_result = pd.DataFrame(df, columns=["Edad","Nombre"])

    # print(df.dtypes)

    # df["Edad"] = df["Edad"].apply(pd.to_numeric)
    # df["Edad"] = df["Edad"].astype(str)

    # (df.dtypes)

    # for column in df:
    #    print('Nombre de la columna: ', column)
    #    print('Nombre de la columna: ', df[column].dtype)

    # datetime.now().strftime("%Y-%m-%d")
    # fecfin =  datetime.now() - timedelta(1)
    # fecst= fecfin.strftime("%Y-%m-%d")

    with beam.Pipeline(options=pipeline_options) as p:
        """
        lectura = (
                    p  # | 'Initializing' >> beam.Create(["1"])
                    | 'Read table BQ' >> beam.io.ReadFromBigQuery(
                #table="Tabla_Fuente",
                #dataset="control",
                table=known_args.name_tabla_fuente,
                dataset=known_args.dataset_tabla_fuente.get(),
                project=known_args.proyect_flujo.get(),
                #table=known_args.name_tabla_fuente,
                #dataset=known_args.dataset_tabla_fuente,
                #project=known_args.proyect_flujo.get(),
                #flatten_results=True,
                #query="SELECT * FROM " + known_args.dataset_tabla_fuente.get() + "." + known_args.name_tabla_fuente.get() + " where Activo = 'si'"
                # " where Componente =  'Preventas'"
            )
        )
        """

        #table_names_dict = beam.pvalue.AsList(lectura)

        r = (
                #lectura
                p | 'Initializing' >> beam.Create(["1"])
                | 'Setting up Instance' >> beam.ParDo(setenv(), known_args=known_args)
                | 'Parametric reading' >> beam.ParDo(ParametricReading(), known_args=known_args)
                | 'Extract delta data' >> beam.ParDo(ExtractDeltaData(), known_args=known_args)
                | 'Part STG' >> beam.ParDo(save_to_bigquery(), known_args)
                | 'Create STG' >> beam.ParDo(create_stg(), known_args)
                | 'Save ODS' >> beam.ParDo(save_to_production(), known_args)
        )

        """
        lectura = (
                p
                # | 'Create' >> beam.Create(['Start'])  # workaround to kickstart the pipeline
                | 'Read table BQ' >> beam.io.ReadFromBigQuery(  # table="tableSalesforce", dataset="testSaleforce",
            project=known_args.proyect_flujo.get(),
            query="SELECT * FROM " + known_args.dataset_tabla_fuente.get() + "." + known_args.name_tabla_fuente.get() + " where Activo = 'si'"
            # " where Componente =  'Preventas'"
                )
            # | beam.Map(print)
        )
        proceso = lectura | 'Get API data' >> beam.ParDo(get_api_data(), known_args)
        crear_delta = proceso | 'Crear Delta' >> beam.ParDo(save_to_bigquery(), known_args)
        escribir = crear_delta | 'Guardar' >> beam.ParDo(save_to_production(), known_args)
        """

        result = p.run()

        logging.info("terminado")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()