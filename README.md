# Dependency Finder

## Para que sirve

Este programa se encarga de buscar las dependencias de las vistas materializadas
y asi crear un orden de refresco para que todas las vistas se refresquen de forma
correcta.

## Setup

Para que el programa funcione es necesario crear en la carpeta principal un archivo
llamado ".env". Dentro de este hay que definir los siguientes parametros.

- DB_NAME=**Nombre de la base de datos a la que te quieres conectar**
- DB_USER=**Nombre de usuario de la base de datos**
- DB_PASS=**Contraseña para el usuario establecido en DB_USER**
- DB_HOST=**Nombre del host o direccion IP**
- DB_PORT=**Numero de puerto de la maquina remota**

Este programa esta desarrollado con python. Para ejecutarlo necesitas
tener instalado python en tu equipo.

Este programa hace uso de los siguientes paquetes

## Uso

Una vez realizado de manera correcta el setup puedes ejecutar el programa
de dos formas:

- Desde un editor de codigo puedes ejecutar el archivo **main.py**
- Desde una consola parado en la carpeta principal puedes usar el comando **python3 main.py** (Ten en cuenta que el comando puede cambiar dependiendo de la version de python instalada).


