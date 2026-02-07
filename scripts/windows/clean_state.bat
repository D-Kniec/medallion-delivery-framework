@echo off
SETLOCAL

cd /d "%~dp0..\.."

echo Cleaning Data Directories...



if exist "data\silver" (
    echo Deleting data\silver...
    rmdir /s /q "data\silver"
)

if exist "data\checkpoints" (
    echo Deleting data\checkpoints...
    rmdir /s /q "data\checkpoints"
)

echo Creating empty structure...
if not exist "data\silver" mkdir "data\silver"
if not exist "data\checkpoints" mkdir "data\checkpoints"

echo Done.
timeout /t 3