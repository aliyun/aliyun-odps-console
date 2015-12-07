@echo off
SETLOCAL ENABLEDELAYEDEXPANSION
@set basedir="%~dp0"
pushd %basedir%

@set mainclass=com.aliyun.openservices.odps.console.ODPSConsole
@set libpath=%cd%\..\lib\
@set confpath=%cd%\..\conf\
@set classpath=.;!confpath!
for %%F in ("%libpath%"*.jar) do (
@set classpath=!classpath!;"%%F"
)

rem set java env
popd
java -Xms64m -Xmx512m -classpath !classpath! %mainclass% %*

ENDLOCAL
