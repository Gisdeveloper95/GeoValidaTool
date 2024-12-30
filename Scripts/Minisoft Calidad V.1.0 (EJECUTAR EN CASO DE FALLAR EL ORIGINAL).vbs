Set shell = CreateObject("WScript.Shell")
batPath = WScript.ScriptFullName
batPath = Left(batPath, Len(batPath) - Len(WScript.ScriptName)) & "GeoValidaTool V.1.0 (if_fail ).bat"
shell.Run """" & batPath & """", 0, false
