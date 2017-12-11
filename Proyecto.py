
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
get_ipython().magic(u'matplotlib inline')


# Encabezados del archivo

# In[2]:


datos = pd.read_csv('pollution_us_2000_2016.csv')
list(datos)


# Numero de datos

# In[3]:


datos.shape


# Las primeras 5 filas

# In[4]:


datos.head()


# Todas las líneas

# In[5]:


datos


# # Transformación de datos

# Los datos fueron transformados para ser de nuevo visualizados ya que en el formato original no se podían visualizar de manera correcta. Se crearon 4 archivos CSV los cuáles cuentan con la información necesaria de cada contaminante por lo que es más fácil su análisis

# Se utilizó pig con los siguientes scripts:

# ```
# inputfile= load '/user/cloudera/pollution_us_2000_2016.csv' using PigStorage(',');
# 
# NO2 = foreach inputfile generate $7 as ciudad, $8 as fecha, $9 as unidades, $10 as promedio;
# 
# store NO2 into '/user/cloudera/prueba_NO2' USING PigStorage (',');
# ```

# ```
# inputfile= load '/user/cloudera/pollution_us_2000_2016.csv' using PigStorage(',');
# 
# NO2 = foreach inputfile generate $7 as ciudad, $8 as fecha, $14 as unidades, $15 as promedio;
# 
# store NO2 into '/user/cloudera/prueba_O3' USING PigStorage (',');
# ```

# ```
# inputfile= load '/user/cloudera/pollution_us_2000_2016.csv' using PigStorage(',');
# 
# NO2 = foreach inputfile generate $7 as ciudad, $8 as fecha, $19 as unidades, $20 as promedio;
# 
# store NO2 into '/user/cloudera/prueba_SO2' USING PigStorage (',');
# ```

# ```
# inputfile= load '/user/cloudera/pollution_us_2000_2016.csv' using PigStorage(',');
# 
# NO2 = foreach inputfile generate $7 as ciudad, $8 as fecha, $24 as unidades, $25 as promedio;
# 
# store NO2 into '/user/cloudera/prueba_CO' USING PigStorage (',');
# ```

# In[6]:


co = pd.read_csv('CO/CO.csv', index_col = 0, parse_dates=True, usecols= ['CO Mean', 'Date Local'])
co.head()


# In[7]:


co.plot(figsize=(20,4))


# In[8]:


co.plot.box()


# In[9]:


co.describe()


# In[10]:


ts_co = co.groupby([co.index.year,
                    co.index.month]).mean()


# In[11]:


ts_co.plot(figsize= (20,4))


# Se puede observar que existen periodos en los cuales el contaminante CO se hace presente, pero también se observa como cada vez existe menos contaminación de CO, los picos altos disminuyen su altura mientras pasa el tiempo.

# ## 1.- De esta gráfica se obtendrá cual es el promedio de contaminación en el año del 2000 y cúal es el promedio de contaminación en el 2010

# In[12]:


no2 = pd.read_csv('NO2/NO2.csv', index_col = 0, parse_dates=True, usecols= ['NO2 Mean', 'Date Local'])
no2.head()


# In[13]:


no2.plot(figsize=(20,4))


# In[14]:


no2.plot.box()


# In[15]:


no2.describe()


# In[16]:


ts_no2 = no2.groupby([no2.index.year,
                    no2.index.month]).mean()


# In[17]:


ts_no2.plot(figsize= (20,4))


# Al igual que en el gráfico anterior en este gráfico se puede observar como hay periodos en los que baja la contaminación de NO2, pero a difeencia del gráfico anterior existe un alto índice de contaminación entre los años 2006 y 2008

# ## 2.- Se quiere investigar la ubicación y la fecha exacta en el que pasó el alto nivel de contaminación entre el 2006 y 2008

# In[18]:


o3 = pd.read_csv('O3/O3.csv', index_col = 0, parse_dates=True, usecols= ['O3 Mean', 'Date Local'])
o3.head()


# In[19]:


o3.plot(figsize=(20,4))


# In[20]:


o3.plot.box()


# In[21]:


o3.describe()


# In[22]:


ts_o3 = o3.groupby([o3.index.year,
                    o3.index.month]).mean()


# In[23]:


ts_o3.plot(figsize= (20,4))


# En este gráfico a diferencia de los otros dos no existe alguna disminución de contaminación a travez del tiempo, aunque si existe una periodicidad.

# ## 3.- De aqui se quiere obtener cuál es la ciudad más contaminada en O3

# In[24]:


so2 = pd.read_csv('SO2/SO2.csv', index_col = 0, parse_dates=True, usecols= ['SO2 Mean', 'Date Local'])
so2.head()


# In[25]:


so2.plot(figsize=(20,4))


# In[26]:


so2.plot.box()


# In[27]:


so2.describe()


# In[28]:


ts_so2 = so2.groupby([so2.index.year,
                    so2.index.month]).mean()


# In[29]:


ts_so2.plot(figsize= (20,4))


# Al parecer en este gráfico tenemos un dato exageradamente alto, mostranto un comportamiento muy parecido al de las gráficas 1 y 2 siendo periódico y disminuyendo la contaminación de SO2. Solo que en este gráfico se puede observar que en ocaciones la contaminación disminuye drásticamente y de nuevo vuelve a subir drásticamente

# ## 4.- De esta gráfica se tiene que identificar el dato que se muestra exagerado y eliminarlo para poder observar bien la gráfica

# ## 5.- Se quiere obtener el promedio de contaminación de SO2 de cada mes

# In[30]:


from pymongo import MongoClient
import json
conn = MongoClient()
db = conn.Pollution
for row in datos.iterrows():
    db.pollution.insert_one({'city': row[1]['City'],
                     'address': row[1]['Address'],
                     'date_local': row[1]['Date Local'],
                     'co_units': row[1]['CO Units'],
                     'co_mean': row[1]['CO Mean'],
                     'no2_units': row[1]['NO2 Units'],
                     'no2_mean': row[1]['NO2 Mean'],
                     'o3_units': row[1]['O3 Units'],
                     'o3_mean': row[1]['O3 Mean'],
                     'so2_units': row[1]['SO2 Units'],
                     'so2_mean': row[1]['SO2 Mean']
                     })


# ## Obtener cual es el promedio de contaminación en el año del 2000 y cúal es el promedio de contaminación en el 2010

# In[31]:


from bson.code import Code
mapFunc = Code("""function(){
        if (this.date_local.endsWith("2000")){
            emit("2000",this.co_mean);
        }else if (this.date_local.endsWith(2010)){
            emit("2010",this.co_mean);        
        }
}""")
reduceFunc = Code("""function(palabras, values){
                 var res = 0;
                 values.forEach(function(v){ res += v})
                 return res / values.length;
}""")
db.pollution.map_reduce(mapFunc,reduceFunc,"co", full_response = True)
promedios = db.co.find()
print("Promedio en 2000: " + str(promedios[0]["value"]))
print("Promedio en 2010: " + str(promedios[1]["value"]))


# ## Investigar la ubicación y la fecha exacta en el que pasó el alto nivel de contaminación entre el 2006 y 2008

# In[32]:


from bson.code import Code
mapFunc = Code("""function(){
        var fecha = new Date(this.date_local);
        var dosmilseis = new Date("2006-01-01")
        var dosmilocho = new Date("2008-12-31")
        if (fecha > dosmilseis && fecha < dosmilocho){
            emit("mayor",this.no2_mean);
        }
}""")
reduceFunc = Code("""function(palabras, values){
                 var res = 0;
                 values.forEach(function(v){
                     if (res < v)
                     res = v;
                 })
                 return res;
}""")
db.pollution.map_reduce(mapFunc,reduceFunc,"no2", full_response = True)
consulta = db.no2.find()
nivel_alto = consulta[0]["value"]
print("Nivel: " + str(nivel_alto))
ciudad = db.pollution.find({"no2_mean": nivel_alto})[1]
print("Fecha: " + ciudad["date_local"])
print("Ubicacion: " + ciudad["address"] + " " + ciudad["city"])


# ## Obtener cuál es la ciudad más contaminada en O3

# In[33]:


from bson.code import Code
mapFunc = Code("""function(){
            emit(this.city,this.o3_mean);
}""")
reduceFunc = Code("""function(palabras, values){
                 var res = 0;
                 values.forEach(function(v){ res += v})
                 return res / values.length;
}""")
db.pollution.map_reduce(mapFunc,reduceFunc,"o3", full_response = True)
consulta = db.o3.find()
mayor = 0
ciudad = ""
for data in consulta:
    if mayor < data["value"]:
        mayor = data["value"]
        ciudad = data["_id"]
print("La ciudad mas contaminada de O3 " + ciudad + " con " + str(mayor) + " de promedio")


# ## Identificar el dato que se muestra exagerado y eliminarlo para poder observar bien la gráfica

# In[34]:


mapFunc = Code("""function(){
            emit("mayor",this.so2_mean);
}""")
reduceFunc = Code("""function(palabras, values){
                 var res = 0;
                 values.forEach(function(v){
                     if (res < v)
                     res = v;
                 })
                 return res;
}""")
db.pollution.map_reduce(mapFunc,reduceFunc,"so2", full_response = True)
consulta = db.so2.find()
nivel_alto = consulta[0]["value"]
print("Nivel: " + str(nivel_alto))
ciudad = db.pollution.find({"so2_mean": nivel_alto})[1]
print("Fecha: " + ciudad["date_local"])
print("Ubicacion: " + ciudad["address"] + " " + ciudad["city"])
db.pollution.delete_one({"so2": nivel_alto})


# In[35]:


mapFunc = Code("""function(){
            emit("mayor",this.so2_mean);
}""")
reduceFunc = Code("""function(palabras, values){
                 var res = 0;
                 values.forEach(function(v){
                     if (res < v)
                     res = v;
                 })
                 return res;
}""")
db.pollution.map_reduce(mapFunc,reduceFunc,"so2", full_response = True)
consulta = db.so2.find()
nivel_alto = consulta[0]["value"]
print("Nivel: " + str(nivel_alto))
ciudad = db.pollution.find({"so2_mean": nivel_alto})[1]
print("Fecha: " + ciudad["date_local"])
print("Ubicacion: " + ciudad["address"] + " " + ciudad["city"])


# In[36]:


db.pollution.delete_one({"so2_mean": nivel_alto})


# In[37]:


mapFunc = Code("""function(){
        var meses = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'];
        var fecha = new Date(this.date_local);
        emit(meses[fecha.getMonth()], this.so2_mean)
}""")
reduceFunc = Code("""function(palabras, values){
                 var res = 0;
                 values.forEach(function(v){
                     res = res + v;
                 })
                 return res / values.length;
}""")
db.pollution.map_reduce(mapFunc,reduceFunc,"so2_2", full_response = True)
resultado = db.so2_2.find()
for row in resultado:
    print(row["_id"] + ":\t" + str(row['value']))

