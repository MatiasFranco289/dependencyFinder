import os
import sys
import time
import threading
import psycopg2
from dotenv import load_dotenv
import colorama
from colorama import Fore
from datetime import datetime

colorama.init()
load_dotenv()

timerFlag = False
timerThread = None
matViewsName = set()
matViewsData = []
refreshOrder = []
tableNames = []

# Conectarse a la base de datos
print(Fore.WHITE + "Por favor introduzca los datos de de la DB")
print("---------------------------")

dbName = input("Nombre de la DB: ")
dbUser = input("Usuario: ")
dbPass = input("Contraseña: ")
dbHost = input("Host: ")
dbPort = input("Puerto: ")

print("-----------------------")
print("Conectando a la DB ..")

try:
    # Intento conectar
    conn = psycopg2.connect(database=dbName, user=dbUser, password=dbPass, host=dbHost, port=dbPort)
    cursor = conn.cursor()
except Exception as e:
    print(Fore.RED + "No se puedo establencer conexion con la DB.")
    print("La siguiente excepcion ha ocurrido mientras se intentaba conectar: ")
    print(" ")
    print(e)
    sys.exit()
else:
    print(Fore.GREEN + "Conexion establecida correctamente.")


def getTableNames():
    global tableNames

    print(Fore.WHITE + "Obteniendo informacion de tablas ..")

    try:
        cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")
        tables = cursor.fetchall()
        tableNames = [''.join(str(elemento) for elemento in tupla) for tupla in tables]
    except Exception as e:
        print(Fore.RED + "No se puedo obtener la informacion de las tablas.")
        print("La siguiente excepcion ha ocurrido: ")
        print("")
        print(e)
        sys.exit()
    else:
        print(Fore.GREEN + "Informacion obtenida correctamente.")


def getMatViewsDefinition():
    # Obtengo el codigo fuente de todas las vistas materializadas
    global matViewsData, matViewsName

    print(Fore.WHITE + "Obteniendo informacion de vistas materializadas ..")

    try:
        cursor.execute("SELECT matviewname FROM pg_matviews")
        matviews = cursor.fetchall()
    except Exception as e:
        print(Fore.RED + "No se pudo obtener la informacion de las vistas materializadas.")
        print("La siguiente excepcion ha ocurrido: ")
        print("")
        print(e)
        sys.exit()
    else:
        print(Fore.GREEN + "Informacion obtenida correctamente.")

    print(Fore.WHITE + "Obteniendo codigo fuente de vistas materializadas ..")

    for matview in matviews:
        matViewName = matview[0]
        matViewsName.add(matViewName)
        try:
            cursor.execute(f"SELECT definition FROM pg_matviews WHERE matviewname = '{matViewName}'")
            matViewDefinition = cursor.fetchone()[0]
        except Exception as e:
            print(Fore.RED + f"No se puedo obtener el codigo fuente para la vista materializada {matViewName}.")
            print("La siguiente excepcion ha ocurrido: ")
            print("")
            print(e)
            sys.exit()

        matViewsData.append({
            "name": matViewName,
            "definition": matViewDefinition,
            "dependencies": set()
        })

    print(Fore.GREEN + "Codigo fuente obtenido correctamente.")


def sanitizeWord(word):
    # Eliminar caracteres demas en palabras
    undesiredCharacters = ["(", ")", ";"]

    for character in undesiredCharacters:
        word = word.replace(character, "")

    if word != "": return word


def getMatViewsDependencies():
    global matViewsData
    keyword = ["from", "join", "FROM", "JOIN"]
    undesiredDependencies = []

    # Por cada vista
    for matview in matViewsData:
        viewWords = matview["definition"].split()

        # Por cada palabras en su codigo fuente
        for i in range(len(viewWords)):
            actualWord = viewWords[i]
            # Cualquier palabra despues de un FROM o JOIN no vacia es agregada como dependencia
            if (keyword.__contains__(actualWord)):
                nextWord = sanitizeWord(viewWords[i + 1])
                if (nextWord != None):
                    matview["dependencies"].add(nextWord)

            # Como se agregan todas las palabras despues de un FROM puede ser que esa dependencia
            # Sea una dependencia agregada con la clausula WITH en cuyo caso no me interesaria porque
            # No es ni una tabla ni una vista ni una vista materializada
            # Asi que las agrego a una lista para depues poder filtrarlas y eliminarlas
            if(actualWord == "WITH"):
                undesiredDependencies.append(sanitizeWord(viewWords[i + 1]))

        # Filtro y elimino dependencias agregadas por WITH
        matview["dependencies"] = tuple(x for x in matview["dependencies"] if not undesiredDependencies.__contains__(x))
def isFundamentalView(dependencies):
    # Esta funcion determina si una vista es fundamental o no
    # Una vista fundamental si no hace uso de otras vistas
    global matViewsName

    for dependency in dependencies:
        ## Si la lista que contiene el nombre de todas las vistas contiene el nombre de aunque sea una de la dependencias
        ## de la vista, devuelve false
        if (matViewsName.__contains__(dependency)):
            return False
    return True


def isDependancy(usedDependencies, validDependencies):
    # Esta funcion determina si las dependencias de una vista se encuentran en la lista de dependencias validas

    for dependency in usedDependencies:
        if not validDependencies.__contains__(dependency) and not tableNames.__contains__(dependency):
            return False
    return True


def setMatViewsPriority():
    global refreshOrder
    totalViewsAmount = len(matViewsData)

    print("")
    print(Fore.WHITE + "Estableciendo orden de refresco ..")
    print("")

    i = 0
    while i < len(matViewsData):
        actualmatview = matViewsData[i]
        if isDependancy(actualmatview["dependencies"], refreshOrder):
            refreshOrder.append(actualmatview["name"])
            print(actualmatview["name"])
            del matViewsData[i]
            i = -1
        i += 1

    print("")
    print(
        Fore.GREEN + f"Orden establecido para un total de {len(refreshOrder)} de {totalViewsAmount} vistas materializadas.")

    if totalViewsAmount != len(refreshOrder):
        print(Fore.RED + f"No se pudo establecer el orden para un total de {len(matViewsData)} vistas materializadas.")
        print("")
        for view in matViewsData:
            print(view["name"])


def checkUserConfirmation(question):
    validRespones = ["Y", "N"]
    print(Fore.WHITE + "----------------")
    print("")
    userResponse = input(question)

    while not validRespones.__contains__(userResponse):
        print(Fore.RED + "Resupuesta no valida. Introduza Y para confirmar o N para negar.")
        userResponse = input()

    if userResponse != "Y":
        sys.exit()


def startTimer():
    global timerFlag, timerThread
    timerFlag = True
    timerThread = threading.Thread(target=timer)
    timerThread.start()


def stopTimer():
    global timerFlag, timerThread
    timerFlag = False
    timerThread.join()

def getMatViewCount(matviewName):
    try:
        query = f'SELECT COUNT(*) FROM "{matviewName}";'
        cursor.execute(query)
        viewRecordsAmount = cursor.fetchone()[0]
        return viewRecordsAmount
    except Exception as e:
        return "ERROR"

def refreshAll():
    for view in refreshOrder:

        logger(["", "---"])
        logger([view])
        logger(["---"])
        logger([f"Cantidad de registros anterior: {getMatViewCount(view)}"])

        query = f'REFRESH MATERIALIZED VIEW "{view}";'
        print(Fore.WHITE + "Resfrescando vista materializada " + Fore.CYAN + f"{view}" + Fore.WHITE + ".")
        startTimer()

        try:
            cursor.execute(query)
            conn.commit()
        except Exception as e:
            stopTimer()
            print(Fore.RED + f"No se pudo refrescar {view}.")
            print("La siguiente excepcion ha ocurrido mientras se intentaba refrescar: ")
            print("")
            print(e)
            sys.exit()
        else:
            stopTimer()
            print(Fore.CYAN + f"{view} " + Fore.GREEN + "refrescada correctamente.")
            print("")
            logger([f"Cantidad de registros actual: {getMatViewCount(view)}"])

        logger(["---", ""])

def timer():
    elapsedTime = 0

    while timerFlag:
        print(f"Refrescando {elapsedTime}s")
        elapsedTime += 1
        time.sleep(1)

def logger(messages):
    # Esta funcion escribe en un archivo de texto la cantidad de registros en vistas materializadas
    # antes y despues del refresco
    # Recibe un array de string

    for message in messages:
        try:
            with open("log.txt", 'a') as file:
                file.write(message + '\n')
        except FileNotFoundError:
            with open("log.txt", 'w') as file:
                file.write(message + '\n')

getTableNames()
getMatViewsDefinition()
getMatViewsDependencies()
setMatViewsPriority()
checkUserConfirmation("Esta seguro que desea refrescar estas vistas materializadas? [Y,N]: ")
logger(["----------", f"{datetime.now().strftime('%d-%m-%Y')} {datetime.now().strftime('%H:%M:%S')}", "----------"])
refreshAll()
logger(["", ""])
print("")
conn.close()
print(Fore.WHITE + "Ejecucion terminada.")
