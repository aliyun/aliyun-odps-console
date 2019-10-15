@echo off

java -version > nul 2>&1
if %errorlevel% neq 0 (
   echo no java found
   PAUSE
)

set argC=0
for %%x in (%*) do Set /A argC+=1

SETLOCAL ENABLEDELAYEDEXPANSION
@set basedir="%~dp0"
pushd %basedir%

@set mainclass=com.aliyun.openservices.odps.console.ODPSConsole
@set libpath=%cd%\..\lib\
@set mrlibpath=%cd%\..\lib\mapreduce-api.jar
@set confpath=%cd%\..\conf\
@set classpath=.;!confpath!;!mrlibpath!;!libpath!\*

rem set java env
popd
java -Xms64m -Xmx512m -classpath "!classpath!" %mainclass% %*

if errorlevel 1 (
  if %argC% == 0 (
   PAUSE
  )
)

ENDLOCAL
