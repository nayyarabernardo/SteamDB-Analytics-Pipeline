@echo off

REM Defina o horário desejado para a execução (9:00)
set hora_desejada=09:00

REM Obtenha a hora atual
set hora_atual=%TIME:~0,5%

REM Compare a hora atual com o horário desejado
if "%hora_atual%" leq "%hora_desejada%" (
    python C:\Users\nayya\Downloads\Estudo\projetos\desafio-beAnalytic-engdadosjr\scripts\main.py
    echo Executado com sucesso!
) else (
    echo Horário de execução não alcançado. O arquivo não será executado.
)

pause
