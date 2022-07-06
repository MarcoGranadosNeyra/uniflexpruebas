# COMANDOS UTILES PARA PRUEBAS Y NUEVOS DESPLIEGUES

# instalar dependencias
sudo -H pip3 install --upgrade pip
pip3 install -r requirements.txt
pip3  install pandas
pip3 install apache_beam

https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-python

Registrar service
set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\lenovo\PycharmProjects\dataflowUniflex\service-account.json

#Comando correr local
python main.py --runner DataflowRunner --project pe-udps-datalake-prd --region us-east1 --temp_location gs://uniflex_dataflow_files/temp/ --requirements_file requirements.txt --machine_type n1-standard-4 --subnetwork https://www.googleapis.com/compute/v1/projects/pe-udps-networking-trv/regions/us-east1/subnetworks/subnet-us-east1 --job_name etl-uniflex3 --num_workers 1 --proyect_flujo pe-udps-datalake-prd --dataset_tabla_fuente control --name_tabla_fuente Tabla_Fuente --cadenaserver jdbc:sqlserver://10.13.0.4\SQL2016:1433 --userserver xertica-bi --passserver Xertica*2022 --bucketfiles uniflex_dataflow_files

#Crear plantilla
Template
python main.py --runner DataflowRunner --template_location gs://uniflex_dataflow_files/templates/prd_etl_u niflex_v1 --project pe-udps-datalake-prd --region us-east1 --temp_location gs://uniflex_dataflow_files/temp/ --requirements_file requirements.txt --machine_type n1-standard-4 --subnetwork https://www.googleapis.com/compute/v1/projects/pe-udps-networking-trv/regions/us-east1/subnetworks/subnet-us-east1 --proyect_flujo pe- udps-datalake-prd  --bucketfiles uniflex_dataflow_files  --dataset_tabla_fuente Configuration  --name_tabla_fuente Tabla_Fuente

#URL para Cloud Scheduler
"https://dataflow.googleapis.com/v1b3/projects/py-ali-finanzas-pe-rrff-dev/locations/us-east1/templates:launch?gcsPath=gs://etl_dataflow_files/templates/prd_etl_alicorp_v1"

