
    #Paso 1
        # clonar repositorio 
        # git clone repositorio.git

    #Paso 2
        # Crear Nuestro bucket
        # Subir el archibo a nuestro bucket
        # gsutil cp muestra.txt gs://muestratxt-bucket

    #Paso 3 Crear el DataFlow

        # python main.py --entrada gs://muestratxt-bucket/data/muestra.txt --salida gs://muestratxt-bucket/out/muestra.txt --runner DataFlowRunner --temp_location gs://muestratxt-bucket/temp/ --region us-central1 --project project147-354617
