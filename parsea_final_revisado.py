#!/usr/bin/python
# -*- encoding: utf-8 -*-

import sys
import MySQLdb
import re
import os
import fnmatch
import datetime
#from datetime import timedelta

# Establecemos la conexión con la base dedatos MySQL de predymol
db = MySQLdb.Connect("xxx.xxx.xxx.xxx", "root", "password", "predymoldb")
cursor = db.cursor()

# Leemos el fichero "lastday.txt" donde tendremos almacenada la última fecha en la que leimos un fichero de log
# Ejemplos:
# numfecha: 2013-09-16
# ultimafecha: 20130916
f1 = open("lastday.txt", 'r')
numfecha = f1.read()
ultimafecha = numfecha.replace("-", "")
f1.close()

# Guardaremos en la variable "fechahoy" el valor de la fecha de hoy, quitando previamente el caracter "-"
# Ejemplos:
# now: 2013-09-19 HH:MM:SS
# fechahoy: 20130923
now = datetime.datetime.now()
fechahoy = now.strftime("%Y%m%d")

# Leeremos el fichero "lastline.txt" donde tendremos almacenada la última línea parseada dentro del fichero de log "x"
f2 = open("lastline.txt", 'r')
numlinea = f2.read()
f2.close()

# Convertimos de String a Int las variables:
# 1) "ultimafecha" => 20130916
# 2) "fechahoy" => 20130923
# 3) "numlinea" => X
a = int(numlinea)
b = int(fechahoy)
c = int(ultimafecha)

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# WHILE: Mientras el día analizado sea diferente al de hoy, habrá que analizar todos los logs anteriroes "idp-process-*.log". De estos ficheros se analizarán
#        todas las líneas.
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# Comprobaremos si la fecha actual es más reciente a la que teníamos almacenada en "ultimafecha"
# En caso AFIRMATIVA, analizaremos el fichero "idp-process-<numfecha>.log" a partir de la línea "numlinea" (extraida del fichero "lastline.txt"

while b > c:
    for file in os.listdir('c:\\opt\shibboleth-idp\logs'):
        if fnmatch.fnmatch(file, 'idp-process-' + numfecha + '.log'):

            # Con el siguiente comando eliminaremos las primeras "numlinea" número de líneas y guararemos el resto de lineas en un nuevo fichero llamado "logreducido"
            comando = ("c:\\etxahun\\UnxUtils\\usr\\local\\wbin\\tail.exe -n +" + str(numlinea) + " c:\opt\shibboleth-idp\logs\\" + file + " > logreducido.txt")
            os.system(comando)

            try:
                for line in open('logreducido.txt', 'r'):
                    a += 1
                    if "Authentication" in line:
                        #Con esta RegExp buscamos la cadena "Authentication" en la línea.
                        m = re.search('Authentication (\w+) for dn: uid=(\w+),', line)

                        #Con esta RegExp buscamos la hora [HH:MM:SS] dentro de la línea.
                        m2 = re.search('(\d+:\d+:\d+).\d+ -', line)
                        hora = m2.group(1)
                        horabuena = hora.replace(":", "")
                        horaregistro = ultimafecha + horabuena + "000"

                        # Con esta query buscamos el ID de cada usuario. Para ello, consultamos la tabla "Users" de la BBDD.
                        sql = "SELECT id FROM Users WHERE UID='%s'" % (m.group(2))
                        cursor.execute(sql)
                        userid = cursor.fetchone()

                        # Inicializamos la variable "success" a TRUE.
                        success = 1

                        # En caso de que la línea analizada contenga "FAILED", cambiamos el valor de la variable "success"a FALSE.
                        if m.group(1) == 'failed':
                            success = 0

                        # Con esta query hacemos inserción en la tabla "Shibboleth_Access" de la BBDD MySQL de predymol, los datos recien parseados.
                        sql2 = "INSERT INTO shibboleth_access(Datetime,UserId,Success,Info) VALUES ('%d', '%s', '%s', null)" % (int(horaregistro), userid[0], success)
                        cursor.execute(sql2)
                        db.commit()

                    elif "Search" in line:
                        #Con esta RegExp buscamos la cadena "Search" en la línea.
                        m3 = re.search('Search for user: (\w+) failed', line)

                        #Con esta RegExp buscamos la hora [HH:MM:SS] dentro de la línea.
                        m4 = re.search('(\d+:\d+:\d+).\d+ -', line)
                        hora2 = m4.group(1)
                        horabuena2 = hora2.replace(":", "")
                        horaregistro2 = numfecha + horabuena2 + "000"

                        # Con esta query hacemos registramos el intento de acceso fallido de usuarios que no estan dados de alta en el LDAP.
                        sql3 = "INSERT INTO shibboleth_access(Datetime,UserId,Success,Info) VALUES ('%d', null, '0', 'uid=%s')" % (int(horaregistro2), m3.group(1))
                        cursor.execute(sql3)
                        db.commit()

            except IOError:
                print (("File '" + file + "' not found."))
                sys.exit(-1)

    # Convertimos de nuevo la fecha de INT a formato fecha 2013-09-16 e incrementamos +1 dias: 2013-09-16 --> 2013-09-17
    # ultimafecha: 20130916
    # print c
    c += 1  # c = int(ultimafecha)
    d = str(c)
    numfecha = d[0:4] + "-" + d[4:6] + "-" + d[6:]

    # Guardamos en "lastday" el último día analizado
    f3 = open('lastday.txt', 'w')
    f3.write(numfecha)
    f3.close()

    # Pondremos el valor de "numlinea" a 0 porque mientras no llegue el dia de hoy, habra que analizar todas las lineas de los ficheros de log, es decir,
    # sin suprimir ninguna línea
    numlinea = 0
    f4 = open('lastline.txt', 'w')
    f4.write(str(numlinea))
    f4.close()


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
# A continuación se analiza el caso en el que la fecha a analizar coincida con el día actual (NOW)
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------

# Leemos el fichero "lastday.txt" donde tendremos almacenado el último día analizado
f5 = open("lastday.txt", 'r')
numfecha = f5.read()
nuevafecha = numfecha.replace("-", "")
f5.close()

# Leemos el fichero "lastline.txt" donde tendremos que almacenada la última línea leida
f6 = open("lastline.txt", 'r')
numlinea = f6.read()
f6.close()
lineaseliminar = int(numlinea) + 1

# Con el siguiente comando eliminaremos las primeras "numlinea" número de líneas y guardaremos el resto de líneas en un nuevo fichero llamado "logreducido"
comando = ("C:\\etxahun\\UnxUtils\\usr\\local\\wbin\\tail.exe -n +" + str(lineaseliminar) + " c:\opt\shibboleth-idp\logs\idp-process.log > logreducido.txt")
os.system(comando)

# Convertimos de String a Int el numero de línea
contador = int(numlinea)

if b == int(nuevafecha):
    try:
        for line in open('logreducido.txt', 'r'):
            contador += 1
            if "Authentication" in line:
                #Con esta RegExp buscamos la cadena "Authentication" en la línea.
                m = re.search('Authentication (\w+) for dn: uid=(\w+),', line)

                #Con esta RegExp buscamos la hora [HH:MM:SS] dentro de la línea.
                m2 = re.search('(\d+:\d+:\d+).\d+ -', line)
                hora = m2.group(1)
                horabuena = hora.replace(":", "")
                fechahoy = now.strftime("%Y%m%d")
                horaregistro = fechahoy + horabuena + "000"

                # Con esta query buscamos el ID de cada usuario. Para ello, consultamos la tabla "Users" de la BBDD.
                sql = "SELECT id FROM Users WHERE UID='%s'" % (m.group(2))
                cursor.execute(sql)
                userid = cursor.fetchone()

                # Inicializamos la variable "success" a TRUE.
                success = 1

                # En caso de que la línea analizada contenga "FAILED", cambiamos el valor de la variable "success"a FALSE.
                if m.group(1) == 'failed':
                    success = 0

                # Con esta query hacemos inserción en la tabla "Shibboleth_Access" de la BBDD MySQL de predymol, los datos recien parseados.
                sql2 = "INSERT INTO shibboleth_access(Datetime,UserId,Success,Info) VALUES ('%d', '%s', '%s', null)" % (int(horaregistro), userid[0], success)
                cursor.execute(sql2)
                db.commit()

            elif "Search" in line:
                #Con esta RegExp buscamos la cadena "Search" en la línea.
                m3 = re.search('Search for user: (\w+) failed', line)

                #Con esta RegExp buscamos la hora [HH:MM:SS] dentro de la línea.
                m4 = re.search('(\d+:\d+:\d+).\d+ -', line)
                hora2 = m4.group(1)
                horabuena2 = hora2.replace(":", "")
                fechahoy = now.strftime("%Y%m%d")
                horaregistro2 = fechahoy + horabuena2 + "000"

                # Con esta query hacemos registramos el intento de acceso fallido de usuarios que no estan dados de alta en el LDAP.
                sql3 = "INSERT INTO shibboleth_access(Datetime,UserId,Success,Info) VALUES ('%d', null, '0', 'uid=%s')" % (int(horaregistro2), m3.group(1))
                cursor.execute(sql3)
                db.commit()

        #if int(numlinea) == 0:
        #    f7 = open('lastline.txt', 'w')
        #    f7.write(str(contador))
        #    f7.close()
        #else:
        #    f8 = open('lastline.txt', 'w')
        #    f8.write(str(contador))
        #    f8.close()

        f7 = open('lastline.txt', 'w')
        f7.write(str(contador))
        f7.close()

        # Cerramos el "cursor" y la conexión con la base de datos.
        cursor.close()
        db.close()

    except IOError:
        print (("File '" + file + "' not found."))
        sys.exit(-1)