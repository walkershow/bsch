Set args = WScript.Arguments
If args.Count = 2 Then
    servid= WScript.Arguments(0)
    gid = WScript.Arguments(1) 
End If
set WshShell=WScript.CreateObject("WScript.Shell")  
strDesktop=WshShell.SpecialFolders("Desktop")  
set oShellLink=WshShell.CreateShortcut(strDesktop & "\shutdown.lnk")  
oShellLink.TargetPath="z:\"&servid&"\w"&gid&"\script\shutdown.bat"  
oShellLink.WindowStyle=1  
oShellLink.Hotkey="CTRL+ALT+O"  
' oShellLink.IconLocation=""  
oShellLink.Description="shutdown vm"  
oShellLink.WorkingDirectory="z:\"&servid&"\w"&gid&"\script\"
oShellLink.Save  

set oShellLink2=WshShell.CreateShortcut(strDesktop & "\savestate.lnk")  
oShellLink2.TargetPath="z:\"&servid&"\w"&gid&"\script\savestate.bat"  
oShellLink2.WindowStyle=1  
oShellLink2.Hotkey="CTRL+ALT+P"  
' oShellLi2nk.IconLocation=""  
oShellLink2.Description="savestate vm"  
oShellLink2.WorkingDirectory="z:\"&servid&"\w"&gid&"\script\"
oShellLink2.Save  

Dim fso
Set fso = CreateObject("scripting.filesystemobject")
if fso.FileExists("d:\pm\02\test.bat") Then fso.deleteFile "d:\pm\02\test.bat"
Set myfile=fso.CreateTextFile("d:\pm\02\test.bat",ture)
myfile.WriteLine "choice /t 1 /d y /n >nul"
myfile.WriteLine "start cmd /k ""cd/d z:\"&servid&"\w"&gid&"\script&&wssc.bat"""
myfile.WriteLine "choice /t 1 /d y /n >nul"
myfile.WriteLine "start z:\a\"&servid&"\a\a\AWin2.exe"
myfile.WriteLine "zhixing Z:\JB\w"&gid&"\jb\20170411.jb 100 C:\Users\Administrator\Desktop 1.0"
myfile.Close
set myfile = nothing
set WshShell = nothing
