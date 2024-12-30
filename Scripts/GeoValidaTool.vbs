' GeoValidaTool.vbs con control anti-desesperados
Option Explicit

' Variables globales
Const TIEMPO_ESPERA = 30 ' segundos
Dim ws, fso, scriptDir, rootDir, batPath, iconPath, shortcutPath

' Mensajes satiricos para usuarios impacientes
Function ObtenerMensajeSatirico()
    Dim mensajes(9)
    ' Mensajes originales
    mensajes(0) = "Ey Ey Ey! El programa no va mas rapido por mas que le hagas clic. Calma No comas ansias, Esto toma su tiempo"
    mensajes(1) = "Sabias que dar clic como loco no acelera nada? Tambien intente hacer que mi cafe se enfriara mas rapido soplandole... spoiler: no funciono"
    mensajes(2) = "Me pregunto que terminaria primero: el programa abriendose o tu encontrando la paciencia..."
    mensajes(3) = "Darle multiples clics es como pedirle a tu jefe que te suba el sueldo... no va a pasar mas rapido"
    mensajes(4) = "Tantos clics? Tambien le das asi de duro al F5 cuando esperas el deposito de tu sueldo?"
    
    ' Mensajes adicionales
    mensajes(5) = "Calma... el programa es como el delivery: llegara cuando tenga que llegar, no cuando tu ansiedad lo desee"
    mensajes(6) = "Hey!!! El programa es como Windows Update: llegara cuando quiera, no cuando lo necesites"
    mensajes(7) = "En serio? Tantos clics? Ni mi router parpadea tanto cuando falla la conexion"
    mensajes(8) = "Calma! Das mas clics que mi tia compartiendo cadenas de WhatsApp!"
    mensajes(9) = "Si la paciencia fuera oro, ya estarias en bancarrota... Espera un momento!"
    
    ' Seleccionar mensaje aleatorio
    Randomize
    ObtenerMensajeSatirico = mensajes(Int(Rnd * 10))
End Function

' Verificar si ya esta en ejecucion
Function YaEstaEjecutando()
    Dim tempFile
    tempFile = "C:\Temp\GeoValidaToolLock.tmp"
    
    ' Crear directorio temporal si no existe
    If Not fso.FolderExists("C:\Temp") Then
        fso.CreateFolder("C:\Temp")
    End If
    
    ' Verificar si existe el archivo de bloqueo
    If fso.FileExists(tempFile) Then
        Dim archivo, ultimaEjecucion
        Set archivo = fso.GetFile(tempFile)
        ultimaEjecucion = CDbl(archivo.DateLastModified)
        
        ' Verificar si han pasado menos de 30 segundos
        If DateDiff("s", ultimaEjecucion, Now) < TIEMPO_ESPERA Then
            MsgBox ObtenerMensajeSatirico(), vbInformation + vbOKOnly, "Ten paciencia!"
            YaEstaEjecutando = True
            Exit Function
        End If
    End If
    
    ' Actualizar o crear archivo de bloqueo
    Set archivo = fso.CreateTextFile(tempFile, True)
    archivo.Close
    YaEstaEjecutando = False
End Function

' Programa principal
Sub Main()
    ' Inicializar objetos
    Set ws = CreateObject("WScript.Shell")
    Set fso = CreateObject("Scripting.FileSystemObject")
    
    ' Si ya esta ejecutandose, salir
    If YaEstaEjecutando() Then
        WScript.Quit
    End If
    
    ' Obtener rutas
    scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
    rootDir = fso.GetParentFolderName(scriptDir)
    batPath = scriptDir & "\run.bat"
    iconPath = scriptDir & "\img\icono.ico"
    shortcutPath = rootDir & "\GeoValidaTool.lnk"
    
    ' Crear acceso directo
    Dim shortcut
    Set shortcut = ws.CreateShortcut(shortcutPath)
    shortcut.TargetPath = WScript.ScriptFullName
    shortcut.IconLocation = iconPath
    shortcut.WorkingDirectory = rootDir
    shortcut.Save
    
    ' Ejecutar el bat
    ws.Run """" & batPath & """", 0, True
End Sub

' Ejecutar el programa
Main()